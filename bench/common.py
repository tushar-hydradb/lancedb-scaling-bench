"""Shared helpers for the LanceDB scaling benches.

Design notes that matter for reading the results:

* **Two commit modes.** MOVEIT prod writes plain ``s3://`` on MinIO with an
  UnsafeCommitHandler (single-writer-per-connector). We reproduce that as
  ``safe=False``. The transferable multi-writer story needs an external commit
  store; Lance supports that via the ``s3+ddb://...?ddbTableName=`` dataset URI,
  which we reproduce as ``safe=True`` against DynamoDB-local.

* **Two API levels.** The write/read/2gb/optimize benches use the high-level
  ``lancedb`` package (how MOVEIT uses it). The concurrency bench needs precise
  control of the commit-store URI, so it drops to the ``lance`` package and
  builds the full ``<...>/<name>.lance`` dataset URI itself.

* **Metrics.** Peak RSS is sampled from a background thread via ``psutil`` so it
  captures native (Rust) allocations, not just Python heap. CPU-seconds come
  from ``psutil.Process().cpu_times()``. The container is capped by compose; if
  an op needs more than the cap it OOM-kills — that kill IS a finding.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field, asdict
from typing import Any

import boto3
import numpy as np
import psutil
import pyarrow as pa

# --- environment (set by docker-compose, or run_ec2.sh for real S3) ---------
# BENCH_S3_REAL=1 switches from the local MinIO stack to real AWS S3: creds come
# from the default AWS credential chain (EC2 IAM instance role), the endpoint is
# AWS's own, and there's no DynamoDB commit store (real S3 has native atomic
# conditional-PUT, so plain s3:// single-writer-per-table is safe).
REAL_S3 = os.environ.get("BENCH_S3_REAL") == "1"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
DDB_ENDPOINT = os.environ.get("DDB_ENDPOINT", "http://ddb:8000")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET = os.environ.get("BENCH_BUCKET", "lance-bench")
DDB_COMMIT_TABLE = os.environ.get("DDB_COMMIT_TABLE", "lance_commits")
CAP_LABEL = os.environ.get("BENCH_CAP_LABEL", "unknown")

# Container mounts /results; a bare-venv EC2 run points this at a local dir.
RESULTS_DIR = os.environ.get("BENCH_RESULTS_DIR", "/results")


# --- storage options / URIs -------------------------------------------------
def storage_options(safe: bool) -> dict[str, str]:
    """object_store options. Real S3 (BENCH_S3_REAL) carries region only — the
    AWS default credential chain (EC2 instance role) supplies creds and AWS
    resolves the HTTPS endpoint. MinIO needs an explicit endpoint, static keys
    and ``allow_http`` (plain HTTP); in safe mode it also points Lance's
    DynamoDB commit store at DynamoDB-local."""
    if REAL_S3:
        return {"region": REGION}
    opts = {
        "endpoint": S3_ENDPOINT,
        "region": REGION,
        "access_key_id": ACCESS_KEY,
        "secret_access_key": SECRET_KEY,
        "allow_http": "true",
    }
    if safe:
        # Lance's external-manifest DynamoDB store reads this endpoint.
        opts["dynamodb_endpoint"] = DDB_ENDPOINT
    return opts


def db_uri(prefix: str) -> str:
    """High-level lancedb.connect() URI (a directory holding tables)."""
    return f"s3://{BUCKET}/{prefix}"


def dataset_uri(prefix: str, name: str, safe: bool) -> str:
    """Full lance dataset URI. In safe mode the DynamoDB commit-store table is
    attached via the scheme + query string that Lance recognises."""
    base = f"s3://{BUCKET}/{prefix}/{name}.lance"
    if safe:
        return base.replace("s3://", "s3+ddb://", 1) + f"?ddbTableName={DDB_COMMIT_TABLE}"
    return base


# --- schemas ----------------------------------------------------------------
def moveit_schema() -> pa.Schema:
    """MOVEIT's drain-buffer schema, verbatim from src/sync/merge.rs:270-286.
    ``data`` holds the entire raw record; here it's a plain string column (the
    JSON extension type isn't needed to characterise throughput / the 2GB cap).
    """
    return pa.schema(
        [
            ("id", pa.string()),
            ("connector_id", pa.string()),
            ("object_type", pa.string()),
            ("op", pa.string()),
            ("content_hash", pa.string()),
            ("ingested_at", pa.timestamp("us")),
            ("cursor", pa.int64()),
            ("data", pa.string()),
        ]
    )


def moveit_schema_large() -> pa.Schema:
    """Same as moveit_schema but ``data`` is large_string (64-bit offsets),
    which lifts the 2GB-per-batch cap on that column."""
    fields = [f for f in moveit_schema()]
    fields[-1] = pa.field("data", pa.large_string())
    return pa.schema(fields)


def vector_schema(dim: int = 768) -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.string()),
            ("vector", pa.list_(pa.float32(), dim)),
            ("category", pa.string()),
            ("score", pa.float64()),
        ]
    )


# --- DynamoDB commit table (safe mode) --------------------------------------
def ensure_ddb_commit_table() -> None:
    """Create the external-manifest table Lance's DDB commit store expects:
    hash key ``base_uri`` (S), range key ``version`` (N). Idempotent."""
    ddb = boto3.client(
        "dynamodb",
        endpoint_url=DDB_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )
    existing = ddb.list_tables().get("TableNames", [])
    if DDB_COMMIT_TABLE in existing:
        return
    ddb.create_table(
        TableName=DDB_COMMIT_TABLE,
        KeySchema=[
            {"AttributeName": "base_uri", "KeyType": "HASH"},
            {"AttributeName": "version", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "base_uri", "AttributeType": "S"},
            {"AttributeName": "version", "AttributeType": "N"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.get_waiter("table_exists").wait(TableName=DDB_COMMIT_TABLE)


# --- S3 footprint probe -----------------------------------------------------
def _s3_client():
    if REAL_S3:
        # Default chain (instance role) + AWS-resolved endpoint.
        return boto3.client("s3", region_name=REGION)
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )


@dataclass
class S3Footprint:
    total_bytes: int
    object_count: int
    fragment_count: int  # *.lance data files


def s3_footprint(prefix: str, name: str) -> S3Footprint:
    """Sum object bytes + count data fragments under a table's S3 prefix."""
    key_prefix = f"{prefix}/{name}.lance/"
    s3 = _s3_client()
    total = 0
    objs = 0
    frags = 0
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": key_prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            total += o["Size"]
            objs += 1
            k = o["Key"]
            if "/data/" in k and k.endswith(".lance"):
                frags += 1
        if resp.get("IsTruncated"):
            token = resp["NextContinuationToken"]
        else:
            break
    return S3Footprint(total_bytes=total, object_count=objs, fragment_count=frags)


# --- metering ---------------------------------------------------------------
@dataclass
class Metrics:
    wall_s: float = 0.0
    cpu_s: float = 0.0
    peak_rss_mb: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)


class Meter:
    """Context manager: wall time, CPU-seconds, and sampled peak RSS (MB).

    RSS is sampled from a daemon thread every 50ms so native allocations that
    never touch the Python heap are still captured.
    """

    def __init__(self, sample_hz: float = 20.0):
        self._proc = psutil.Process()
        self._interval = 1.0 / sample_hz
        self._peak = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.metrics = Metrics()

    def _sample(self) -> None:
        while not self._stop.is_set():
            try:
                rss = self._proc.memory_info().rss
                if rss > self._peak:
                    self._peak = rss
            except psutil.Error:
                pass
            self._stop.wait(self._interval)

    def __enter__(self) -> "Meter":
        self._peak = self._proc.memory_info().rss
        self._cpu0 = sum(self._proc.cpu_times()[:2])  # user + system
        self._t0 = time.perf_counter()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.metrics.wall_s = time.perf_counter() - self._t0
        self.metrics.cpu_s = sum(self._proc.cpu_times()[:2]) - self._cpu0
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        self.metrics.peak_rss_mb = round(self._peak / 1e6, 1)


# --- container-level (cgroup v2) time-series sampler ------------------------
# psutil.Process only sees one process tree; for the scaling graph we need the
# WHOLE container (all instances), so we read cgroup v2 directly. cpu.stat's
# usage_usec is cumulative container CPU time; memory.current is live bytes.
CGROUP = "/sys/fs/cgroup"


def _cgroup_cpu_usec() -> int | None:
    try:
        with open(f"{CGROUP}/cpu.stat") as fh:
            for line in fh:
                if line.startswith("usage_usec"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


def _cgroup_mem_bytes() -> int | None:
    try:
        with open(f"{CGROUP}/memory.current") as fh:
            return int(fh.read().strip())
    except OSError:
        return None


def cgroup_cpu_limit_cores() -> float | None:
    """Cores available: the cgroup cpu.max quota if capped, else the physical
    core count (uncapped EC2 run — the reference line on graphs is then the box
    size, and CgroupSampler measures whole-instance CPU since the box is
    dedicated to the bench)."""
    try:
        with open(f"{CGROUP}/cpu.max") as fh:
            quota, period = fh.read().split()
        if quota != "max":
            return int(quota) / int(period)
    except OSError:
        pass
    n = os.cpu_count()
    return float(n) if n else None


class CgroupSampler:
    """Samples container CPU (cores) + RSS (MB) on a background thread.

    * ``mark(label, **meta)`` annotates an instant (e.g. "instance 3 added").
    * ``extra_fn`` (optional) returns a dict merged into every sample — used by
      the scaling bench to record active-instance count and aggregate op count.
    Each sample: {t, cpu_cores, rss_mb, **extra}.
    """

    def __init__(self, interval: float = 0.25, extra_fn=None):
        self.interval = interval
        self.extra_fn = extra_fn
        self.samples: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0 = 0.0

    def _now(self) -> float:
        return time.perf_counter() - self._t0

    def _loop(self) -> None:
        prev_usec = _cgroup_cpu_usec()
        prev_wall = time.perf_counter()
        while not self._stop.is_set():
            self._stop.wait(self.interval)
            usec = _cgroup_cpu_usec()
            wall = time.perf_counter()
            cores = 0.0
            if usec is not None and prev_usec is not None and wall > prev_wall:
                cores = (usec - prev_usec) / 1e6 / (wall - prev_wall)
            prev_usec, prev_wall = usec, wall
            mem = _cgroup_mem_bytes() or 0
            sample = {"t": round(self._now(), 3), "cpu_cores": round(cores, 3),
                      "rss_mb": round(mem / 1e6, 1)}
            if self.extra_fn is not None:
                try:
                    sample.update(self.extra_fn())
                except Exception:  # noqa: BLE001 — never let sampling crash the run
                    pass
            self.samples.append(sample)

    def mark(self, label: str, **meta: Any) -> None:
        self.events.append({"t": round(self._now(), 3), "label": label, **meta})

    def __enter__(self) -> "CgroupSampler":
        self._t0 = time.perf_counter()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)


# --- result IO --------------------------------------------------------------
def write_result(name: str, payload: dict[str, Any]) -> None:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    payload = {
        "bench": name,
        "cap_label": CAP_LABEL,
        "unix_time": time.time(),
        **payload,
    }
    path = os.path.join(RESULTS_DIR, f"{name}.json")
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=2, default=_json_default)
    print(f"[{name}] wrote {path}", flush=True)


def _json_default(o: Any) -> Any:
    if hasattr(o, "__dataclass_fields__"):
        return asdict(o)
    raise TypeError(f"not JSON serialisable: {type(o)}")


def metrics_dict(m: Metrics) -> dict[str, Any]:
    return {
        "wall_s": round(m.wall_s, 4),
        "cpu_s": round(m.cpu_s, 4),
        "peak_rss_mb": m.peak_rss_mb,
        **m.extra,
    }


# --- data generators --------------------------------------------------------
def make_blob_rows(n: int, payload_bytes: int, key_start: int = 0, connector_id: str = "bench") -> pa.RecordBatch:
    """A batch of MOVEIT-shaped rows whose ``data`` column is ``payload_bytes``
    of JSON-ish text each. Built column-wise for speed."""
    now_us = int(time.time() * 1_000_000)
    filler = "x" * max(0, payload_bytes - 40)
    ids = [f"{connector_id}:msg:{key_start + i}" for i in range(n)]
    data = ['{"k":"%d","v":"%s"}' % (key_start + i, filler) for i in range(n)]
    arrays = [
        pa.array(ids, pa.string()),
        pa.array([connector_id] * n, pa.string()),
        pa.array(["messages"] * n, pa.string()),
        pa.array(["upsert"] * n, pa.string()),
        pa.array([f"h{key_start + i}" for i in range(n)], pa.string()),
        pa.array([now_us] * n, pa.timestamp("us")),
        pa.array(list(range(key_start, key_start + n)), pa.int64()),
        pa.array(data, pa.string()),
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=moveit_schema())


INT32_MAX = 2_147_483_647


def make_blob_rows_gaussian(
    n: int,
    mean_bytes: float,
    std_bytes: float,
    min_bytes: int,
    key_start: int,
    connector_id: str,
    rng: "np.random.Generator",
    schema: pa.Schema | None = None,
) -> pa.RecordBatch:
    """A batch of MOVEIT-shaped rows whose ``data`` payloads are drawn from a
    gaussian (mean/std bytes, clamped >= min_bytes). Memory-safe at multi-MB row
    sizes: the ``data`` values buffer is one exact allocation wrapped via
    ``from_buffers`` (int32 offsets for plain string, int64 for large_string) —
    no per-row Python string list (which would double peak RSS). The int32
    offset cap is asserted, mirroring the 2GB-per-array limit in
    ``bench_2gb_column.py``.

    Callers track logical bytes written via ``batch.column('data').nbytes``
    (== the values buffer size + tiny offset overhead).
    """
    schema = schema or moveit_schema()
    is_large = pa.types.is_large_string(schema.field("data").type)

    sizes = np.clip(rng.normal(mean_bytes, std_bytes, n), min_bytes, None).astype(np.int64)
    total = int(sizes.sum())
    if not is_large and total >= INT32_MAX:
        raise ValueError(
            f"batch data bytes {total:,} >= int32 offset cap {INT32_MAX:,}; "
            f"reduce rows-per-array (n={n}) or use moveit_schema_large()"
        )

    off_dtype = np.int64 if is_large else np.int32
    offsets = np.empty(n + 1, dtype=off_dtype)
    offsets[0] = 0
    offsets[1:] = np.cumsum(sizes).astype(off_dtype)

    # Single ~mean*n allocation of filler; sliced by offsets into per-row cells.
    vbuf = pa.py_buffer(b"x" * total)
    obuf = pa.py_buffer(offsets.tobytes())
    data_type = pa.large_string() if is_large else pa.string()
    data_arr = pa.Array.from_buffers(data_type, n, [None, obuf, vbuf])

    now_us = int(time.time() * 1_000_000)
    keys = range(key_start, key_start + n)
    arrays = [
        pa.array([f"{connector_id}:msg:{k}" for k in keys], pa.string()),
        pa.array([connector_id] * n, pa.string()),
        pa.array(["messages"] * n, pa.string()),
        pa.array(["upsert"] * n, pa.string()),
        pa.array([f"h{k}" for k in keys], pa.string()),
        pa.array([now_us] * n, pa.timestamp("us")),
        pa.array(np.arange(key_start, key_start + n, dtype=np.int64), pa.int64()),
        data_arr,
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
