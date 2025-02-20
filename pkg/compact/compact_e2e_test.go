package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/objtesting"
	"github.com/thanos-io/thanos/pkg/testutil"
	"gopkg.in/yaml.v2"
)

func TestSyncer_SyncMetas_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		relabelConfig := make([]*relabel.Config, 0)
		sy, err := NewSyncer(nil, nil, bkt, 0, 1, false, relabelConfig)
		testutil.Ok(t, err)

		// Generate 15 blocks. Initially the first 10 are synced into memory and only the last
		// 10 are in the bucket.
		// After the first synchronization the first 5 should be dropped and the
		// last 5 be loaded from the bucket.
		var ids []ulid.ULID
		var metas []*metadata.Meta

		for i := 0; i < 15; i++ {
			id, err := ulid.New(uint64(i), nil)
			testutil.Ok(t, err)

			var meta metadata.Meta
			meta.Version = 1
			meta.ULID = id

			if i < 10 {
				sy.blocks[id] = &meta
			}
			ids = append(ids, id)
			metas = append(metas, &meta)
		}
		for _, m := range metas[5:] {
			var buf bytes.Buffer
			testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
			testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), metadata.MetaFilename), &buf))
		}

		groups, err := sy.Groups()
		testutil.Ok(t, err)
		testutil.Equals(t, ids[:10], groups[0].IDs())

		testutil.Ok(t, sy.SyncMetas(ctx))

		groups, err = sy.Groups()
		testutil.Ok(t, err)
		testutil.Equals(t, ids[5:], groups[0].IDs())
	})
}

func TestSyncer_GarbageCollect_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Generate 10 source block metas and construct higher level blocks
		// that are higher compactions of them.
		var metas []*metadata.Meta
		var ids []ulid.ULID

		relabelConfig := make([]*relabel.Config, 0)

		for i := 0; i < 10; i++ {
			var m metadata.Meta

			m.Version = 1
			m.ULID = ulid.MustNew(uint64(i), nil)
			m.Compaction.Sources = []ulid.ULID{m.ULID}
			m.Compaction.Level = 1

			ids = append(ids, m.ULID)
			metas = append(metas, &m)
		}

		var m1 metadata.Meta
		m1.Version = 1
		m1.ULID = ulid.MustNew(100, nil)
		m1.Compaction.Level = 2
		m1.Compaction.Sources = ids[:4]
		m1.Thanos.Downsample.Resolution = 0

		var m2 metadata.Meta
		m2.Version = 1
		m2.ULID = ulid.MustNew(200, nil)
		m2.Compaction.Level = 2
		m2.Compaction.Sources = ids[4:8] // last two source IDs is not part of a level 2 block.
		m2.Thanos.Downsample.Resolution = 0

		var m3 metadata.Meta
		m3.Version = 1
		m3.ULID = ulid.MustNew(300, nil)
		m3.Compaction.Level = 3
		m3.Compaction.Sources = ids[:9] // last source ID is not part of level 3 block.
		m3.Thanos.Downsample.Resolution = 0

		var m4 metadata.Meta
		m4.Version = 14
		m4.ULID = ulid.MustNew(400, nil)
		m4.Compaction.Level = 2
		m4.Compaction.Sources = ids[9:] // covers the last block but is a different resolution. Must not trigger deletion.
		m4.Thanos.Downsample.Resolution = 1000

		// Create all blocks in the bucket.
		for _, m := range append(metas, &m1, &m2, &m3, &m4) {
			fmt.Println("create", m.ULID)
			var buf bytes.Buffer
			testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
			testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), metadata.MetaFilename), &buf))
		}

		// Do one initial synchronization with the bucket.
		sy, err := NewSyncer(nil, nil, bkt, 0, 1, false, relabelConfig)
		testutil.Ok(t, err)
		testutil.Ok(t, sy.SyncMetas(ctx))

		testutil.Ok(t, sy.GarbageCollect(ctx))

		var rem []ulid.ULID
		err = bkt.Iter(ctx, "", func(n string) error {
			rem = append(rem, ulid.MustParse(n[:len(n)-1]))
			return nil
		})
		testutil.Ok(t, err)

		sort.Slice(rem, func(i, j int) bool {
			return rem[i].Compare(rem[j]) < 0
		})
		// Only the level 3 block, the last source block in both resolutions should be left.
		testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID, m4.ULID}, rem)

		// After another sync the changes should also be reflected in the local groups.
		testutil.Ok(t, sy.SyncMetas(ctx))

		// Only the level 3 block, the last source block in both resolutions should be left.
		groups, err := sy.Groups()
		testutil.Ok(t, err)

		testutil.Equals(t, "0@{}", groups[0].Key())
		testutil.Equals(t, []ulid.ULID{metas[9].ULID, m3.ULID}, groups[0].IDs())
		testutil.Equals(t, "1000@{}", groups[1].Key())
		testutil.Equals(t, []ulid.ULID{m4.ULID}, groups[1].IDs())
	})
}

func TestGroup_Compact_e2e(t *testing.T) {
	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		prepareDir, err := ioutil.TempDir("", "test-compact-prepare")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(prepareDir)) }()

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		var metas []*metadata.Meta
		extLset := labels.Labels{{Name: "e1", Value: "1"}}
		b1, err := testutil.CreateBlock(ctx, prepareDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}, {Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
		}, 100, 0, 1000, extLset, 124)
		testutil.Ok(t, err)

		meta, err := metadata.Read(filepath.Join(prepareDir, b1.String()))
		testutil.Ok(t, err)
		metas = append(metas, meta)

		b3, err := testutil.CreateBlock(ctx, prepareDir, []labels.Labels{
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "a", Value: "5"}},
			{{Name: "a", Value: "6"}},
		}, 100, 2001, 3000, extLset, 124)
		testutil.Ok(t, err)

		// Mix order to make sure compact is able to deduct min time / max time.
		meta, err = metadata.Read(filepath.Join(prepareDir, b3.String()))
		testutil.Ok(t, err)
		metas = append(metas, meta)

		// Currently TSDB does not produces empty blocks (see: https://github.com/prometheus/tsdb/pull/374). However before v2.7.0 it was
		// so we still want to mimick this case as close as possible.
		b2, err := createEmptyBlock(prepareDir, 1001, 2000, extLset, 124)
		testutil.Ok(t, err)

		// blocks" count=3 mint=0 maxt=3000 ulid=01D1RQCRRJM77KQQ4GYDSC50GM sources="[01D1RQCRMNZBVHBPGRPG2M3NZQ 01D1RQCRPJMYN45T65YA1PRWB7 01D1RQCRNMTWJKTN5QQXFNKKH8]".

		meta, err = metadata.Read(filepath.Join(prepareDir, b2.String()))
		testutil.Ok(t, err)
		metas = append(metas, meta)

		// Due to TSDB compaction delay (not compacting fresh block), we need one more block to be pushed to trigger compaction.
		freshB, err := testutil.CreateBlock(ctx, prepareDir, []labels.Labels{
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "a", Value: "5"}},
		}, 100, 3001, 4000, extLset, 124)
		testutil.Ok(t, err)

		meta, err = metadata.Read(filepath.Join(prepareDir, freshB.String()))
		testutil.Ok(t, err)
		metas = append(metas, meta)

		// Upload and forget about tmp dir with all blocks. We want to ensure same state we will have on compactor.
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(prepareDir, b1.String())))
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(prepareDir, b2.String())))
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(prepareDir, b3.String())))
		testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(prepareDir, freshB.String())))

		// Create fresh, empty directory for actual test.
		dir, err := ioutil.TempDir("", "test-compact")
		testutil.Ok(t, err)
		defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

		metrics := newSyncerMetrics(nil)
		g, err := newGroup(
			nil,
			bkt,
			extLset,
			124,
			false,
			metrics.compactions.WithLabelValues(""),
			metrics.compactionFailures.WithLabelValues(""),
			metrics.garbageCollectedBlocks,
		)
		testutil.Ok(t, err)

		comp, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewLogfmtLogger(os.Stderr), []int64{1000, 3000}, nil)
		testutil.Ok(t, err)

		shouldRerun, id, err := g.Compact(ctx, dir, comp)
		testutil.Ok(t, err)
		testutil.Assert(t, !shouldRerun, "group should be empty, but compactor did a compaction and told us to rerun")

		// Add all metas that would be gathered by syncMetas.
		for _, m := range metas {
			testutil.Ok(t, g.Add(m))
		}

		shouldRerun, id, err = g.Compact(ctx, dir, comp)
		testutil.Ok(t, err)
		testutil.Assert(t, shouldRerun, "there should be compactible data, but the compactor reported there was not")

		resDir := filepath.Join(dir, id.String())
		testutil.Ok(t, block.Download(ctx, log.NewNopLogger(), bkt, id, resDir))

		meta, err = metadata.Read(resDir)
		testutil.Ok(t, err)

		testutil.Equals(t, int64(0), meta.MinTime)
		testutil.Equals(t, int64(3000), meta.MaxTime)
		testutil.Equals(t, uint64(6), meta.Stats.NumSeries)
		testutil.Equals(t, uint64(2*4*100), meta.Stats.NumSamples) // Only 2 times 4*100 because one block was empty.
		testutil.Equals(t, 2, meta.Compaction.Level)
		testutil.Equals(t, []ulid.ULID{b1, b3, b2}, meta.Compaction.Sources)

		// Check thanos meta.
		testutil.Assert(t, extLset.Equals(labels.FromMap(meta.Thanos.Labels)), "ext labels does not match")
		testutil.Equals(t, int64(124), meta.Thanos.Downsample.Resolution)

		// Check object storage. All blocks that were included in new compacted one should be removed.
		err = bkt.Iter(ctx, "", func(n string) error {
			id, ok := block.IsBlockDir(n)
			if !ok {
				return nil
			}

			for _, source := range meta.Compaction.Sources {
				if id.Compare(source) == 0 {
					return errors.Errorf("Unexpectedly found %s block in bucket", source.String())
				}
			}
			return nil
		})
		testutil.Ok(t, err)
	})
}

// createEmptyBlock produces empty block like it was the case before fix: https://github.com/prometheus/tsdb/pull/374.
// (Prometheus pre v2.7.0).
func createEmptyBlock(dir string, mint int64, maxt int64, extLset labels.Labels, resolution int64) (ulid.ULID, error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	if err := os.Mkdir(path.Join(dir, uid.String()), os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	if err := os.Mkdir(path.Join(dir, uid.String(), "chunks"), os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	w, err := index.NewWriter(path.Join(dir, uid.String(), "index"))
	if err != nil {
		return ulid.ULID{}, errors.Wrap(err, "new index")
	}

	if err := w.Close(); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "close index")
	}

	m := tsdb.BlockMeta{
		Version: 1,
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{uid},
		},
	}
	b, err := json.Marshal(&m)
	if err != nil {
		return ulid.ULID{}, err
	}

	if err := ioutil.WriteFile(path.Join(dir, uid.String(), "meta.json"), b, os.ModePerm); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "saving meta.json")
	}

	if _, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(dir, uid.String()), metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
	}, nil); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "finalize block")
	}

	return uid, nil
}

func TestSyncer_SyncMetasFilter_e2e(t *testing.T) {
	var err error

	relabelContentYaml := `
    - action: drop
      regex: "A"
      source_labels:
      - cluster
    `
	var relabelConfig []*relabel.Config
	err = yaml.Unmarshal([]byte(relabelContentYaml), &relabelConfig)
	testutil.Ok(t, err)

	extLsets := []labels.Labels{{{Name: "cluster", Value: "A"}}, {{Name: "cluster", Value: "B"}}}

	objtesting.ForeachStore(t, func(t testing.TB, bkt objstore.Bucket) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		sy, err := NewSyncer(nil, nil, bkt, 0, 1, false, relabelConfig)
		testutil.Ok(t, err)

		var ids []ulid.ULID
		var metas []*metadata.Meta

		for i := 0; i < 16; i++ {
			id, err := ulid.New(uint64(i), nil)
			testutil.Ok(t, err)

			var meta metadata.Meta
			meta.Version = 1
			meta.ULID = id
			meta.Thanos = metadata.Thanos{
				Labels: extLsets[i%2].Map(),
			}

			ids = append(ids, id)
			metas = append(metas, &meta)
		}
		for _, m := range metas[:10] {
			var buf bytes.Buffer
			testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
			testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), metadata.MetaFilename), &buf))
		}

		testutil.Ok(t, sy.SyncMetas(ctx))

		groups, err := sy.Groups()
		testutil.Ok(t, err)
		var evenIds []ulid.ULID
		for i := 0; i < 10; i++ {
			if i%2 != 0 {
				evenIds = append(evenIds, ids[i])
			}
		}
		testutil.Equals(t, evenIds, groups[0].IDs())

		// Upload last 6 blocks.
		for _, m := range metas[10:] {
			var buf bytes.Buffer
			testutil.Ok(t, json.NewEncoder(&buf).Encode(&m))
			testutil.Ok(t, bkt.Upload(ctx, path.Join(m.ULID.String(), metadata.MetaFilename), &buf))
		}

		// Delete first 4 blocks.
		for _, m := range metas[:4] {
			testutil.Ok(t, block.Delete(ctx, log.NewNopLogger(), bkt, m.ULID))
		}

		testutil.Ok(t, sy.SyncMetas(ctx))

		groups, err = sy.Groups()
		testutil.Ok(t, err)
		evenIds = make([]ulid.ULID, 0)
		for i := 4; i < 16; i++ {
			if i%2 != 0 {
				evenIds = append(evenIds, ids[i])
			}
		}
		testutil.Equals(t, evenIds, groups[0].IDs())
	})
}
