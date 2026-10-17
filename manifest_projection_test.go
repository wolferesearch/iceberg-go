// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iceberg

// bifrost fork: tests for the read-side statistics projection
// (WithoutColumnStats / KeepBounds / ManifestEntriesWith / ErrProjectedEntry).

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	projPointdateID = 1
	projRegionID    = 2
)

// wideManifest writes a v2 manifest for a table with `cols` columns (pointdate,
// region, then doubles) and `files` data files, every file carrying full
// statistics for every column — the shape of a wide factor table.
func wideManifest(t *testing.T, cols, files int) (ManifestFile, []byte, PartitionSpec, *Schema) {
	t.Helper()
	fields := []NestedField{
		{ID: projPointdateID, Name: "pointdate", Type: DateType{}, Required: true},
		{ID: projRegionID, Name: "region", Type: StringType{}, Required: true},
	}
	for id := 3; id <= cols; id++ {
		fields = append(fields, NestedField{ID: id, Name: fmt.Sprintf("f%d", id), Type: Float64Type{}})
	}
	schema := NewSchema(7, fields...)
	spec := NewPartitionSpecID(0)
	snapshotID, seq := int64(4242), int64(9)

	entries := make([]ManifestEntry, 0, files)
	for i := 0; i < files; i++ {
		sizes := make(map[int]int64, cols)
		counts := make(map[int]int64, cols)
		nulls := make(map[int]int64, cols)
		lower := make(map[int][]byte, cols)
		upper := make(map[int][]byte, cols)
		for id := 1; id <= cols; id++ {
			sizes[id], counts[id], nulls[id] = int64(100+id), 1000, int64(id%7)
			lo, hi := make([]byte, 8), make([]byte, 8)
			switch id {
			case projPointdateID:
				lo, hi = make([]byte, 4), make([]byte, 4)
				binary.LittleEndian.PutUint32(lo, uint32(20000+i))
				binary.LittleEndian.PutUint32(hi, uint32(20000+i))
			case projRegionID:
				lo, hi = []byte("USA"), []byte("USA")
			default:
				binary.LittleEndian.PutUint64(lo, math.Float64bits(-float64(id)))
				binary.LittleEndian.PutUint64(hi, math.Float64bits(float64(id)))
			}
			lower[id], upper[id] = lo, hi
		}
		b, err := NewDataFileBuilder(spec, EntryContentData,
			fmt.Sprintf("s3://bucket/t/data/pointdate=2026-01-%02d/part-%d.parquet", i%28+1, i),
			ParquetFile, map[int]any{}, map[int]string{}, map[int]int{}, 1000, 50_000_000)
		require.NoError(t, err)
		df := b.ColumnSizes(sizes).ValueCounts(counts).NullValueCounts(nulls).
			LowerBoundValues(lower).UpperBoundValues(upper).Build()
		entries = append(entries, NewManifestEntry(EntryStatusADDED, &snapshotID, &seq, &seq, df))
	}
	var buf bytes.Buffer
	mf, err := WriteManifest("s3://bucket/t/metadata/wide-m0.avro", &buf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)

	return mf, buf.Bytes(), spec, schema
}

func collectEntries(t *testing.T, mf ManifestFile, raw []byte, opts ...ManifestReadOption) []ManifestEntry {
	t.Helper()
	var out []ManifestEntry
	for e, err := range iterManifestWith(mf, bytes.NewReader(raw), false, opts...) {
		require.NoError(t, err)
		out = append(out, e)
	}

	return out
}

func TestManifestProjectionKeepsFactsDropsStats(t *testing.T) {
	mf, raw, _, _ := wideManifest(t, 60, 12)
	full := collectEntries(t, mf, raw)
	lite := collectEntries(t, mf, raw, WithoutColumnStats(), KeepBounds(projPointdateID, projRegionID))
	require.Len(t, lite, len(full))

	for i := range full {
		f, l := full[i].DataFile(), lite[i].DataFile()
		require.Equal(t, f.FilePath(), l.FilePath())
		require.Equal(t, f.Count(), l.Count())
		require.Equal(t, f.FileSizeBytes(), l.FileSizeBytes())
		require.Equal(t, f.Partition(), l.Partition())
		require.Equal(t, full[i].Status(), lite[i].Status())
		require.Equal(t, full[i].SequenceNum(), lite[i].SequenceNum())

		require.False(t, ColumnStatsProjected(f))
		require.True(t, ColumnStatsProjected(l))
		require.Len(t, f.ColumnSizes(), 60)
		require.Empty(t, l.ColumnSizes())
		require.Empty(t, l.ValueCounts())
		require.Empty(t, l.NullValueCounts())
		require.Empty(t, l.NaNValueCounts())

		// Bounds: exactly the kept ids, byte-equal to the full read.
		require.Len(t, l.LowerBoundValues(), 2)
		require.Len(t, l.UpperBoundValues(), 2)
		for _, id := range []int{projPointdateID, projRegionID} {
			require.Equal(t, f.LowerBoundValues()[id], l.LowerBoundValues()[id])
			require.Equal(t, f.UpperBoundValues()[id], l.UpperBoundValues()[id])
		}
	}
}

func TestManifestProjectionWithoutBounds(t *testing.T) {
	mf, raw, _, _ := wideManifest(t, 20, 3)
	lite := collectEntries(t, mf, raw, WithoutColumnStats())
	require.Len(t, lite, 3)
	for _, e := range lite {
		require.True(t, ColumnStatsProjected(e.DataFile()))
		require.Empty(t, e.DataFile().LowerBoundValues())
		require.Empty(t, e.DataFile().UpperBoundValues())
		require.Empty(t, e.DataFile().ColumnSizes())
	}
}

func TestManifestProjectionNoOptionsIsTheFullRead(t *testing.T) {
	mf, raw, _, _ := wideManifest(t, 20, 3)
	full, err := ReadManifest(mf, bytes.NewReader(raw), false)
	require.NoError(t, err)
	same := collectEntries(t, mf, raw) // no options
	require.Len(t, same, len(full))
	for i := range full {
		f, s := full[i].DataFile(), same[i].DataFile()
		require.False(t, ColumnStatsProjected(s))
		require.Equal(t, f.ColumnSizes(), s.ColumnSizes())
		require.Equal(t, f.ValueCounts(), s.ValueCounts())
		require.Equal(t, f.LowerBoundValues(), s.LowerBoundValues())
		require.Equal(t, f.UpperBoundValues(), s.UpperBoundValues())
	}
}

func TestManifestWriterRefusesProjectedEntry(t *testing.T) {
	mf, raw, spec, schema := wideManifest(t, 20, 2)
	lite := collectEntries(t, mf, raw, WithoutColumnStats(), KeepBounds(projPointdateID))
	full := collectEntries(t, mf, raw)

	var out bytes.Buffer
	w, err := NewManifestWriter(2, &out, spec, schema, 777)
	require.NoError(t, err)
	err = w.Add(lite[0])
	require.ErrorIs(t, err, ErrProjectedEntry)
	require.ErrorContains(t, err, lite[0].DataFile().FilePath())
	require.ErrorIs(t, w.Existing(lite[1]), ErrProjectedEntry)
	require.ErrorIs(t, w.Delete(lite[1]), ErrProjectedEntry)
	// A fully read entry is still writable.
	require.NoError(t, w.Add(full[0]))
	require.NoError(t, w.Close())
}

// TestManifestProjectionAllocatesLess pins the point of the projection: the
// bytes a reader allocates for a wide manifest fall when the statistics are
// skipped at decode time. Keeping two columns' bounds still decodes every
// bound before trimming (the Avro map cannot be projected by key), so that
// variant sits between the two.
func TestManifestProjectionAllocatesLess(t *testing.T) {
	mf, raw, _, _ := wideManifest(t, 1500, 40)
	measure := func(opts ...ManifestReadOption) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		es := collectEntries(t, mf, raw, opts...)
		require.Len(t, es, 40)
		runtime.ReadMemStats(&after)

		return after.TotalAlloc - before.TotalAlloc
	}
	fullBytes := measure()
	noStats := measure(WithoutColumnStats())
	twoBounds := measure(WithoutColumnStats(), KeepBounds(projPointdateID, projRegionID))
	t.Logf("allocated: full %d B, no stats %d B, no stats + 2 bounds %d B (manifest %d B)", fullBytes, noStats, twoBounds, len(raw))
	require.Less(t, noStats, fullBytes/5, "skipping every statistic should allocate under a fifth of the full read")
	require.Less(t, twoBounds, fullBytes/2, "keeping two bounds should still allocate under half of the full read")
}

func TestKeepBoundsOnFullReadTrimsOnlyBounds(t *testing.T) {
	mf, raw, _, _ := wideManifest(t, 20, 2)
	es := collectEntries(t, mf, raw, KeepBounds(projRegionID))
	for _, e := range es {
		df := e.DataFile()
		require.False(t, ColumnStatsProjected(df))
		require.Len(t, df.ColumnSizes(), 20)
		require.Len(t, df.LowerBoundValues(), 1)
		require.Equal(t, []byte("USA"), df.LowerBoundValues()[projRegionID])
	}
}

func TestProjectEntrySchemaRequiresDataFile(t *testing.T) {
	sc, err := avroParseForTest(`{"type":"record","name":"x","fields":[{"name":"a","type":"int"}]}`)
	require.NoError(t, err)
	_, err = projectEntrySchema(sc, map[string]bool{"column_sizes": true})
	require.Error(t, err)
	require.True(t, errors.Is(err, err)) // shape only: a plain error, not a panic
}
