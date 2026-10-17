# wolferesearch/iceberg-go — bifrost's fork of apache/iceberg-go

Branch `bifrost-v0.6.0` = upstream tag `v0.6.0` + patches bifrost needs before they reach an upstream release.
bifrost's `go.mod` pins a tag of this fork:

    replace github.com/apache/iceberg-go => github.com/wolferesearch/iceberg-go v0.6.0-bifrost.N

## Tags

- `v0.6.0-bifrost.1` (2026-09-21) — `manifest.go`: day(...) partition fields decode to `iceberg.Date` whether the
  manifest writer used a plain Avro int (Athena/Trino) or an int with the `date` logical type, and re-encode as int.
  Upstream main carries `applyDayTransformDates` (commit 0c8e3498c7ca, 2026-09-18, unreleased); this fork adds two lines
  so it holds on the write path (materialize a decoded entry's partition values under the reader's logical types;
  coerce a stray `time.Time` bound for a plain-int field to epoch days). Without it `ReplaceDataFiles`/`AddFiles` on a
  `day()`-partitioned table with Athena-written manifests fails: `avro: field data_file.partition.pointdate_day: cannot
  use time.Time with Avro type int`.

- `v0.6.0-bifrost.2` (2026-10-10) — `manifest.go`: a manifest LIST whose OCF metadata carries no `format-version` key is
  read at the version its writer schema implies (`sequence_number` ⇒ v2, `first_row_id` ⇒ v3) instead of being assumed v1.
  duckdb's Iceberg writer omits the key on every v2 append, so every manifest the list named was refused with
  `manifest file's 'format-version' metadata indicates version 2, but entry from manifest list indicates version 1` —
  the reason a duckdb-written table was "closed to iceberg-go" until a Trino/Athena pack rewrote the list (bifrost
  SCRUM-359/361). Tests: `TestReadManifestListNoFormatVersionV2`, `TestManifestListVersionFromSchema`. Upstream: the
  spec makes the key optional on lists; upstream main still assumes v1 (no issue filed yet).

- `v0.6.0-bifrost.3` (2026-10-17) — read-side statistics projection (bifrost SCRUM-434/435). `manifest.go`:
  `NewManifestReaderWith` / `ManifestEntriesWith` take `ManifestReadOption`s. `WithoutColumnStats()` resolves the file's
  writer schema against a copy of itself without `column_sizes`, `value_counts`, `null_value_counts`, `nan_value_counts`,
  `distinct_counts`, `lower_bounds`, `upper_bounds` (`ocf.WithReaderSchemaFunc` + `avro.Resolve`), so the decoder skips
  those bytes without building values; `KeepBounds(ids...)` keeps the two bound fields and trims them to the ids after
  decode. Every schema-derived fact (field ids, logical types, v1 fallback) still comes from the WRITER schema. A file read
  this way answers `ColumnStatsProjected()` (helper `iceberg.ColumnStatsProjected(df)`), its statistics accessors return
  nil, and `ManifestWriter.Add/Existing/Delete` refuse it with `ErrProjectedEntry` — a projected entry can never be
  written back as a manifest with statistics silently missing. Nothing changes without options; no writer or scan in this
  package uses them. `table.NewManifestEvaluator` exports the manifest-level partition-summary evaluator for callers that
  walk manifests themselves. Why: a 2,465-column table's manifests carry ~15,000 statistics per data file (218 MB for
  4,523 files); a mirror walk that needs only path/partition/record count/two bounds held ~3.5 GB through `PlanFiles`.
  Measured on a synthetic 1,500-column × 40-file manifest: full read 36.5 MB allocated, no statistics 7.2 MB, two bounds
  kept 12.2 MB (the bound maps cannot be projected by key, so they are decoded then trimmed — a decode-time key filter
  via `avro.CustomType` is the next step if needed). Tests: `manifest_projection_test.go`. Upstream: not proposed.

## Making a change

1. Branch from `bifrost-v0.6.0`, patch, `go vet ./...`, commit with a message that names the upstream issue/commit.
2. Tag `v0.6.0-bifrost.<N+1>` (annotated), push branch + tag.
3. In bifrost: bump the `replace` version, `go mod tidy`, run the corrections drills (SCRUM-275) on dev before the fleet.
4. When upstream releases the fix, move bifrost to that release and retire the tag.
