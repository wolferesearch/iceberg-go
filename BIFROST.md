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

## Making a change

1. Branch from `bifrost-v0.6.0`, patch, `go vet ./...`, commit with a message that names the upstream issue/commit.
2. Tag `v0.6.0-bifrost.<N+1>` (annotated), push branch + tag.
3. In bifrost: bump the `replace` version, `go mod tidy`, run the corrections drills (SCRUM-275) on dev before the fleet.
4. When upstream releases the fix, move bifrost to that release and retire the tag.
