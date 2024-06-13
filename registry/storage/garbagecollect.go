package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
)

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

// GCOpts contains options for garbage collector
type GCOpts struct {
	DryRun         bool
	RemoveUntagged bool
}

// MarkAndSweep performs a mark and sweep of registry data
func MarkAndSweep(ctx context.Context, storageDriver driver.StorageDriver, registry distribution.Namespace, opts GCOpts) error {
	// This implementation strives to do a single pass through the underlying
	// storage bucket, marking all blobs and manifests that are in use, and then
	// sweeping all blobs and manifests that are not marked.

	// List possible deletion candidates before marking in-use blobs to
	// avoid a race condition where a blob is uploaded between mark and sweep
	// and its manifest is not considered.

	blobSet := new(gcBlobSet)
	// Stage 1: List the entire storage backend, recording blobs, layers, manifests, and tags in the blobSet.
	if err := gcWalkStorage(ctx, storageDriver, blobSet); err != nil {
		return err
	}
	// Stage 2: Iterate through all the discovered manifests and mark the blobs that are "in use" and should not be deleted.
	if err := gcMarkReachableDigests(ctx, registry, opts, blobSet); err != nil {
		return err
	}
	// Stage 3: Delete any layer, manifest, or blob that was not marked during the previous stage.
	if err := gcDeleteUnreachableObjects(ctx, storageDriver, opts, blobSet); err != nil {
		return err
	}
	return nil
}

func gcWalkStorage(ctx context.Context, storageDriver driver.StorageDriver, blobSet *gcBlobSet) error {
	markStart := time.Now()
	specPath, err := pathFor(blobsPathSpec{})
	if err != nil {
		return err
	}
	rootPath := path.Dir(specPath)
	if err := storageDriver.Walk(ctx, rootPath, func(fileInfo driver.FileInfo) error {
		if fileInfo.IsDir() {
			return nil
		}
		pathType, err := pathFrom(fileInfo.Path())
		if err != nil || pathType == nil {
			return nil
		}
		switch p := pathType.(type) {
		// It is a lucky convenience that "blobs" sorts before "repositories" in the filesystem.
		// This allows us to build the set of 'deletion candidates' before marking in-use blobs.
		// Anything uploaded during GC, too late to be in blobSet, will never be considered for deletion.
		case blobDataPathSpec:
			blobSet.markBlob(p.digest, fileInfo.Size())
		case layerLinkPathSpec:
			blobSet.markLayer(p.digest, p.name)
		case manifestRevisionLinkPathSpec:
			blobSet.markManifest(p.revision, p.name)
		case manifestTagIndexEntryPathSpec:
			blobSet.markTagLink(p.revision, p.name, p.tag)
		case manifestTagCurrentPathSpec:
			blobSet.markTagCurrent(p.name, p.tag)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk repositories: %v", err)
	}
	emit("marked %d blobs and %d repos in %s", len(blobSet.blobs), len(blobSet.tags), time.Since(markStart).Round(time.Millisecond).String())
	return nil
}

func gcMarkReachableDigests(ctx context.Context, registry distribution.Namespace, opts GCOpts, blobSet *gcBlobSet) error {
	newRepo := func(name string) (distribution.ManifestService, distribution.TagService, error) {
		named, err := reference.WithName(name)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse repository name %s: %w", name, err)
		}
		repository, err := registry.Repository(ctx, named)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to construct repository: %w", err)
		}
		manifestService, err := repository.Manifests(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to construct manifest service: %w", err)
		}
		return manifestService, repository.Tags(ctx), nil
	}

	ingestCount := 0
	ingest := func(name string, d digest.Digest) bool {
		if blobSet.blobs[d] != nil &&
			blobSet.blobs[d].repos[name] != nil &&
			blobSet.blobs[d].repos[name].markInUse {
			return true // already marked
		}
		blobSet.markInUse(d, name)
		emit("%s: marking blob %s", name, d)
		ingestCount++
		return false
	}

	ingestRootManifest := func(name string, manifestService distribution.ManifestService, dgst digest.Digest) error {
		// do not visit references if already marked
		if ingest(name, dgst) {
			return nil
		}
		if err := markManifestReferences(ctx, dgst, manifestService, func(d digest.Digest) bool {
			return ingest(name, d)
		}); err != nil {
			return fmt.Errorf("failed to mark manifest references: %v", err)
		}
		return nil
	}

	markStart := time.Now()
	if opts.RemoveUntagged { // Only tagged manifests and their references are reachable.
		for name, tags := range blobSet.tags {
			manifestService, tagService, err := newRepo(name)
			if err != nil {
				return err
			}
			for _, tag := range tags {
				descriptor, err := tagService.Get(ctx, tag)
				if err != nil {
					return fmt.Errorf("failed to get manifest %s: %w", tag, err)
				}
				if err := ingestRootManifest(name, manifestService, descriptor.Digest); err != nil {
					return err
				}
			}
		}
	} else { // All manifests and their layers are reachable
		for dgst, blob := range blobSet.blobs {
			for name, links := range blob.repos {
				if links.linkManifest {
					manifestService, _, err := newRepo(name)
					if err != nil {
						return err
					}
					if err := ingestRootManifest(name, manifestService, dgst); err != nil {
						return err
					}
				}
			}
		}
	}
	emit("marked %d in-use digests in %s", ingestCount, time.Since(markStart).Round(time.Millisecond).String())
	return nil
}

func gcDeleteUnreachableObjects(ctx context.Context, storageDriver driver.StorageDriver, opts GCOpts, blobSet *gcBlobSet) error {
	sweepStart := time.Now()
	var deletedLayers, deletedManifests, deletedBlobs, deletedSize int
	for dgst, blob := range blobSet.blobs {
		dgstInUse := false
		for name, links := range blob.repos {
			if links.markInUse {
				dgstInUse = true
				continue
			}
			// delete unreachable layers
			if links.linkLayer {
				emit("%s: layer %s is unreachable", name, dgst)
				layerLinkPath, err := pathFor(layerLinkPathSpec{name: name, digest: dgst})
				if err != nil {
					return err
				}
				layerEntryPath := path.Dir(layerLinkPath) // delete the directory instead of just the link file
				deletedLayers++
				if !opts.DryRun {
					dcontext.GetLogger(ctx).Infof("Deleting layer link path: %s", layerEntryPath)
					if err := storageDriver.Delete(ctx, layerEntryPath); err != nil {
						if !errors.As(err, new(driver.PathNotFoundError)) {
							return fmt.Errorf("failed to delete layer %s: %w", name, err)
						}
					}
				}
			}
			// delete unreachable manifests
			if links.linkManifest {
				emit("%s: manifest %s is unreachable", name, dgst)
				deletedManifests++

				for _, tag := range links.linkTags {
					tagsPath, err := pathFor(manifestTagIndexEntryPathSpec{name: name, revision: dgst, tag: tag})
					if err != nil {
						return err
					}
					if !opts.DryRun {
						dcontext.GetLogger(ctx).Infof("deleting manifest tag reference: %s", tagsPath)
						if err := storageDriver.Delete(ctx, tagsPath); err != nil {
							if !errors.As(err, new(driver.PathNotFoundError)) {
								return fmt.Errorf("failed to delete manifest tag reference %s: %w", tagsPath, err)
							}
						}
					}
					manifestPath, err := pathFor(manifestRevisionPathSpec{name: name, revision: dgst})
					if err != nil {
						return err
					}
					if !opts.DryRun {
						dcontext.GetLogger(ctx).Infof("deleting manifest: %s", manifestPath)
						if err := storageDriver.Delete(ctx, manifestPath); err != nil {
							if !errors.As(err, new(driver.PathNotFoundError)) {
								return fmt.Errorf("failed to delete manifest %s: %w", name, err)
							}
						}
					}
				}
			}
		}
		// delete unreachable blobs
		if !dgstInUse {
			emit("blob %s is unreachable (%d bytes)", dgst, blob.size)
			deletedBlobs++
			deletedSize += int(blob.size)
			if !opts.DryRun {
				blobPath, err := pathFor(blobPathSpec{digest: dgst})
				if err != nil {
					return fmt.Errorf("failed to get blob path: %w", err)
				}
				dcontext.GetLogger(ctx).Infof("Deleting blob: %s", blobPath)
				err = storageDriver.Delete(ctx, blobPath)
				if err != nil {
					if !errors.As(err, new(driver.PathNotFoundError)) {
						return fmt.Errorf("failed to delete blob %s: %w", dgst, err)
					}
				}
			}
		}
		emit("Visited %d digests and deleted %d layers, %d manifests, %d blobs (%d bytes) in %s",
			len(blobSet.blobs), deletedLayers, deletedManifests, deletedBlobs, deletedSize, time.Since(sweepStart).Round(time.Millisecond).String())
	}
	return nil
}

// markManifestReferences marks the manifest references recursively
func markManifestReferences(ctx context.Context, dgst digest.Digest, manifestService distribution.ManifestService, ingester func(digest.Digest) bool) error {
	manifest, err := manifestService.Get(ctx, dgst)
	if err != nil {
		return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
	}

	descriptors := manifest.References()
	for _, descriptor := range descriptors {

		// do not visit references if already marked
		if skip := ingester(descriptor.Digest); skip {
			continue
		}

		if ok, _ := manifestService.Exists(ctx, descriptor.Digest); ok {
			err := markManifestReferences(ctx, descriptor.Digest, manifestService, ingester)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// gcBlobSet contains state constructed during the initial walk phase of the garbage collector.
// Each blob is marked with its size and repository links that reference it.
//
// The set also contains a map of repository names to their current tags, used to identify
// which manifests are reachable when RemoveUntagged is set. Repositories with no tags
// but containing manifest or layer links will be added with a non-nil empty slice.
//
// An alternative layout would be to separate the global blobs from a map of repository names to all their contents,
// This would ease processing but duplicating the digests would be more memory-intensive for large registries.
type gcBlobSet struct {
	blobs map[digest.Digest]*gcBlob // digest -> references to blob
	tags  map[string][]string       // repo -> current tags
}

type gcBlob struct {
	size  int64
	repos map[string]*gcRepoLinks
}

type gcRepoLinks struct {
	// set on repos where this blob is reachable (semantics depend on RemoveUntagged option)
	markInUse bool
	// existing link files to this blob
	linkLayer    bool
	linkManifest bool
	linkTags     []string
}

// init ensures all the maps are initialized to avoid downstream panics
func (s *gcBlobSet) init(dgst digest.Digest, name string) {
	if s.blobs == nil {
		s.blobs = make(map[digest.Digest]*gcBlob)
	}
	if dgst != "" {
		if s.blobs[dgst] == nil {
			s.blobs[dgst] = &gcBlob{}
		}
		if s.blobs[dgst].repos == nil {
			s.blobs[dgst].repos = make(map[string]*gcRepoLinks)
		}
		if name != "" && s.blobs[dgst].repos[name] == nil {
			s.blobs[dgst].repos[name] = &gcRepoLinks{}
		}
	}
	if s.tags == nil {
		s.tags = make(map[string][]string)
	}
	if name != "" && s.tags[name] == nil {
		s.tags[name] = []string{}
	}
}

func (s *gcBlobSet) markBlob(dgst digest.Digest, size int64) {
	s.init(dgst, "")
	s.blobs[dgst].size = size
}

func (s *gcBlobSet) markLayer(dgst digest.Digest, name string) {
	s.init(dgst, name)
	s.blobs[dgst].repos[name].linkLayer = true
}

func (s *gcBlobSet) markManifest(dgst digest.Digest, name string) {
	s.init(dgst, name)
	s.blobs[dgst].repos[name].linkManifest = true
}

func (s *gcBlobSet) markTagLink(dgst digest.Digest, name string, tag string) {
	s.init(dgst, name)
	if !slices.Contains(s.blobs[dgst].repos[name].linkTags, tag) {
		s.blobs[dgst].repos[name].linkTags = append(s.blobs[dgst].repos[name].linkTags, tag)
	}
}

func (s *gcBlobSet) markTagCurrent(name string, tag string) {
	s.init("", name)
	if !slices.Contains(s.tags[name], tag) {
		s.tags[name] = append(s.tags[name], tag)
	}
}

func (s *gcBlobSet) markInUse(dgst digest.Digest, name string) {
	s.init(dgst, name)
	s.blobs[dgst].repos[name].markInUse = true
}
