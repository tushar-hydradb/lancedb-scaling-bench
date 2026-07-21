//! Does lance 8.0.0's `CompactionOptions` actually change compaction peak-RSS and
//! wall time? This drives the *exact* crate MOVEIT links (lance 8.0.0, arrow 58),
//! so the numbers reflect production, not the pylance 0.33 binding (which doesn't
//! even expose `max_source_fragments` / `io_buffer_size`).
//!
//! Two subcommands:
//!   seed     — write ~SEED_GB as many SMALL fragments (an uncompacted backlog)
//!   compact  — restore the table to the seed version, then run ONE compaction
//!              with the knobs from env, reporting VmHWM (kernel peak RSS) + wall
//!
//! Each `compact` runs as a FRESH process (driver invokes it once per config), so
//! VmHWM is honestly attributable to that config's compaction — no allocator
//! carry-over between runs. Restoring to the seed version first means every config
//! compacts the identical backlog.

use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    Int64Array, RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::{WriteMode, WriteParams};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
use lance::Dataset;

fn env_str(k: &str) -> Option<String> {
    env::var(k).ok().filter(|s| !s.is_empty())
}
fn env_f64(k: &str, d: f64) -> f64 {
    env_str(k).and_then(|s| s.parse().ok()).unwrap_or(d)
}
fn env_usize(k: &str, d: usize) -> usize {
    env_str(k).and_then(|s| s.parse().ok()).unwrap_or(d)
}
fn env_opt_usize(k: &str) -> Option<usize> {
    env_str(k).and_then(|s| s.parse().ok())
}

/// The 8-column MOVEIT drain schema (data kept plain Utf8 — the Json extension is
/// metadata that doesn't affect compaction cost).
fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("connector_id", DataType::Utf8, false),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("content_hash", DataType::Utf8, false),
        Field::new(
            "ingested_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("cursor", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]))
}

fn storage_options() -> HashMap<String, String> {
    // Real AWS S3: region only; creds via the default chain (IAM instance role).
    let mut m = HashMap::new();
    if let Some(r) = env_str("AWS_REGION").or_else(|| env_str("AWS_DEFAULT_REGION")) {
        m.insert("region".to_string(), r);
    }
    m
}

fn write_params(mode: WriteMode, max_rows_per_file: usize) -> WriteParams {
    WriteParams {
        mode,
        max_rows_per_file,
        // No UnsafeCommitHandler: real S3 has native atomic conditional-PUT, and
        // there's a single writer here.
        store_params: Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                storage_options(),
            ))),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Kernel high-water RSS (peak since process start), in MB. Since a `compact`
/// process only opens + restores + compacts, this is the compaction peak.
fn vmhwm_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("VmHWM:")
                    .and_then(|r| r.trim().trim_end_matches("kB").trim().parse::<f64>().ok())
            })
        })
        .map(|kb| kb / 1024.0)
        .unwrap_or(0.0)
}

/// Lazily generates fixed-width rows so seeding never holds the whole table in RAM
/// (one batch = rows_per_batch * mean_bytes, ~1 GB at 200 * 5 MB).
struct BatchGen {
    remaining: usize,
    rows_per_batch: usize,
    mean_bytes: usize,
    cursor: i64,
    schema: SchemaRef,
}

impl Iterator for BatchGen {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let n = self.rows_per_batch.min(self.remaining);
        let base = self.cursor;

        let ids: Vec<String> = (0..n).map(|i| format!("row-{}", base + i as i64)).collect();
        let conn = vec!["bench"; n];
        let otype = vec!["messages"; n];
        let op = vec!["c"; n];
        let chash: Vec<String> = (0..n).map(|i| format!("{:016x}", base + i as i64)).collect();
        let ts: Vec<i64> = (0..n).map(|i| 1_700_000_000_000_000 + base + i as i64).collect();
        let cur: Vec<i64> = (0..n).map(|i| base + i as i64).collect();
        // ~mean_bytes ascii filler per row; first bytes vary per row so fragments
        // aren't trivially identical. Compressible (like the Python bench) — MB/s
        // is logical, not physical; wall + RSS are the honest metrics here.
        let data: Vec<String> = (0..n)
            .map(|i| {
                let mut v = vec![b'x'; self.mean_bytes];
                let tag = format!("{:016x}", base + i as i64);
                v[..tag.len().min(self.mean_bytes)]
                    .copy_from_slice(&tag.as_bytes()[..tag.len().min(self.mean_bytes)]);
                // SAFETY: all bytes are ascii.
                unsafe { String::from_utf8_unchecked(v) }
            })
            .collect();

        self.remaining -= n;
        self.cursor += n as i64;

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(conn)),
                Arc::new(StringArray::from(otype)),
                Arc::new(StringArray::from(op)),
                Arc::new(StringArray::from(chash)),
                Arc::new(TimestampMicrosecondArray::from(ts)),
                Arc::new(Int64Array::from(cur)),
                Arc::new(StringArray::from(data)),
            ],
        );
        Some(batch)
    }
}

async fn seed() -> Result<(), Box<dyn std::error::Error>> {
    let uri = env_str("BENCH_S3_URI").expect("BENCH_S3_URI");
    let seed_gb = env_f64("SEED_GB", 250.0);
    let mean_bytes = env_usize("MEAN_BYTES", 5_000_000);
    let rows_per_array = env_usize("ROWS_PER_ARRAY", 200);
    let total_rows = (seed_gb * 1e9 / mean_bytes as f64).round() as usize;

    eprintln!(
        "[seed] uri={uri} target={seed_gb}GB rows={total_rows} rows/frag={rows_per_array} \
         mean={mean_bytes}B (=> ~{} fragments, all under trpf)",
        total_rows / rows_per_array
    );

    let schema = table_schema();
    let gen = BatchGen {
        remaining: total_rows,
        rows_per_batch: rows_per_array,
        mean_bytes,
        cursor: 0,
        schema: schema.clone(),
    };
    let reader = RecordBatchIterator::new(gen, schema);
    let t0 = Instant::now();
    let ds = Dataset::write(
        reader,
        &uri,
        Some(write_params(WriteMode::Create, rows_per_array)),
    )
    .await?;
    let v = ds.version_id();
    eprintln!(
        "[seed] done rows={total_rows} fragments={} version={v} wall_s={:.1}",
        ds.count_fragments(),
        t0.elapsed().as_secs_f64()
    );
    // Machine-readable: the driver captures this to pass as SEED_VERSION.
    println!("SEED_VERSION={v}");
    Ok(())
}

async fn compact() -> Result<(), Box<dyn std::error::Error>> {
    let uri = env_str("BENCH_S3_URI").expect("BENCH_S3_URI");
    let seed_v: u64 = env_str("SEED_VERSION").expect("SEED_VERSION").parse()?;
    let trpf = env_usize("TRPF", 500);
    let rows_per_array = env_usize("ROWS_PER_ARRAY", 200);
    let label = env_str("LABEL").unwrap_or_else(|| "unlabeled".into());
    let knob_threads = env_opt_usize("KNOB_NUM_THREADS");
    let knob_msf = env_opt_usize("KNOB_MAX_SOURCE_FRAGMENTS");
    let knob_io_mb = env_opt_usize("KNOB_IO_BUFFER_MB");

    // Restore the identical seed backlog so every config compacts the same input.
    let base = DatasetBuilder::from_uri(&uri)
        .with_write_params(write_params(WriteMode::Append, rows_per_array))
        .load()
        .await?;
    let mut ds = base.checkout_version(seed_v).await?;
    ds.restore().await?;
    let frags_before = ds.count_fragments();

    let mut opts = CompactionOptions {
        target_rows_per_fragment: trpf,
        ..Default::default()
    };
    if let Some(n) = knob_threads {
        opts.num_threads = Some(n);
    }
    opts.max_source_fragments = knob_msf;
    if let Some(mb) = knob_io_mb {
        opts.io_buffer_size = Some((mb as u64) * 1024 * 1024);
    }

    let t0 = Instant::now();
    let metrics = compact_files(&mut ds, opts, None).await?;
    let wall_s = t0.elapsed().as_secs_f64();
    let peak_rss_mb = vmhwm_mb();
    let frags_after = ds.count_fragments();

    let out = serde_json::json!({
        "label": label,
        "num_threads": knob_threads,
        "max_source_fragments": knob_msf,
        "io_buffer_mb": knob_io_mb,
        "trpf": trpf,
        "wall_s": wall_s,
        "peak_rss_mb": peak_rss_mb,
        "fragments_before": frags_before,
        "fragments_after": frags_after,
        "fragments_removed": metrics.fragments_removed,
        "fragments_added": metrics.fragments_added,
    });
    println!("{out}");
    eprintln!(
        "[compact] {label}: peak_rss={peak_rss_mb:.0}MB wall={wall_s:.1}s frags {frags_before}->{frags_after}"
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd = env::args().nth(1).unwrap_or_default();
    match cmd.as_str() {
        "seed" => seed().await,
        "compact" => compact().await,
        other => {
            eprintln!("usage: compaction-knobs [seed|compact]  (got {other:?})");
            std::process::exit(2);
        }
    }
}
