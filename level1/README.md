# data_processor.py — Fast Hourly Request-Log Summarizer

A single-file, standard-library-only tool that turns a large HTTP request log into a per-hour, per-endpoint summary: request count, latency percentiles (p50 / p95 / p99), 5xx error count, and error rate in parts per million.

It is built for speed on big files: the log is split into line-aligned byte ranges, parsed in parallel by forked worker processes, and percentiles are computed with memory-efficient integer arrays. If the input does not look like what the fast path expects, the program transparently falls back to a simple reference implementation that produces identical output.

> **Note on naming:** the file is named `data_processor.py`, but its `--help` text refers to it as `a_ingest_fast.py` (its original name). They are the same program.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Requirements & Setup](#2-requirements--setup)
3. [Input and Output Formats](#3-input-and-output-formats)
4. [API Reference](#4-api-reference)
5. [Code Examples](#5-code-examples)
6. [Error Handling & Edge Cases](#6-error-handling--edge-cases)
7. [How It Works](#7-how-it-works)

---

## 1. Overview

**Problem it solves:** given a log file of requests (`requests.log`), produce a compact summary (`summary.txt`) grouped by *(hour, endpoint)* containing:

| Metric | Meaning |
|---|---|
| `count` | Number of requests in that hour for that endpoint |
| `p50`, `p95`, `p99` | Latency percentiles in microseconds (nearest-rank method) |
| `errors` | Number of responses with status 500–599 |
| `errors_ppm` | Error rate in parts per million, `errors * 1_000_000 // count` |

**Design goals**

- **Throughput:** multi-process parsing and ranking, cache-sized chunking, bulk `bytes` operations instead of per-line Python work.
- **Bounded memory:** latencies are stored as unsigned 32-bit `array('I')` buffers; very large groups are ranked by sorting fixed-size pieces and bisecting on value rather than sorting everything at once.
- **Correctness first:** any unexpected input shape, or any worker failure, falls back to a reference algorithm whose output is the defined behaviour.

---

## 2. Requirements & Setup

**Python:** 3.9 or newer (the module uses `os.waitstatus_to_exitcode`, added in 3.9).

**Platform:** POSIX (Linux / macOS) is the supported target. The fast path uses `os.fork`, `os.pread`, and pipes. On platforms without these (e.g. Windows) the fast path raises and the program drops to the single-process reference algorithm.

**Dependencies:** none beyond the standard library.

| Module | Used for |
|---|---|
| `os`, `sys` | File I/O (`pread`, `fork`, pipes), CLI args, environment, cgroup detection |
| `array` | Compact unsigned 32-bit latency storage |
| `bisect` | Locating hour boundaries and percentile ranks |
| `itertools` | `compress` for fast error counting |
| `marshal` | Serialising worker results over pipes |
| `gc` | Garbage collector is disabled during a run |
| `collections` | `Counter` (imported lazily for the low-cardinality histogram path) |
| `datetime` | Timestamp parsing (imported lazily, reference path only) |

**Installation:** none required. Copy `data_processor.py` where you need it.

**Run from the command line**

```bash
python data_processor.py --data /path/to/data --out /path/to/out
```

| Option | Default | Description |
|---|---|---|
| `--data DIR` / `--data=DIR` | `/data` | Directory containing `requests.log` |
| `--out DIR` / `--out=DIR` | `/out` | Directory where `summary.txt` is written (created if missing) |
| `-h`, `--help` | | Print usage and exit |

**Import from Python**

```python
import data_processor

data_processor.main("/path/to/data", "/path/to/out")
```

**Environment variables**

| Variable | Default | Description |
|---|---|---|
| `A_INGEST_WORKERS` | auto-detected | Force the number of worker processes (minimum 1). Auto-detection honours CPU affinity and container cgroup CPU quotas (cgroup v1 and v2). |
| `A_INGEST_CHUNK` | `262144` | Bytes read per parsing chunk. Read once at import time. |

---

## 3. Input and Output Formats

### Input: `<data_dir>/requests.log`

One request per line, exactly five space-separated fields:

```
<timestamp> <method> <endpoint> <status> <latency_us>
```

Example:

```
2024-05-01T10:00:01.123456Z GET /api/users 200 1500
```

| Field | Format |
|---|---|
| `timestamp` | ISO-8601 with a **required fractional-seconds part**, e.g. `2024-05-01T10:00:01.123456Z`. The fast path requires a trailing `Z`; the reference path accepts any `%z` offset (`%Y-%m-%dT%H:%M:%S.%f%z`). |
| `method` | HTTP method (read but not used in the summary) |
| `endpoint` | Request path, no whitespace |
| `status` | Integer HTTP status. 500–599 counts as an error. |
| `latency_us` | Non-negative integer latency in microseconds (must fit in an unsigned 32-bit integer on the fast path) |

### Output: `<out_dir>/summary.txt`

One line per *(hour, endpoint)*, sorted by hour then endpoint:

```
<YYYY-MM-DDTHH>:00:00Z <endpoint> <count> <p50> <p95> <p99> <errors> <errors_ppm>
```

Example:

```
2024-05-01T10:00:00Z /api/orders 1 4200 4200 4200 0 0
2024-05-01T10:00:00Z /api/users 2 1500 9000 9000 1 500000
2024-05-01T11:00:00Z /api/users 2 1200 7000 7000 1 500000
```

**Percentile definition (nearest rank):** for `n` sorted values, the p-th percentile is `sorted[ceil(p * n / 100) - 1]`. Values are always actual observed latencies; there is no interpolation.

---

## 4. API Reference

The module exposes functions rather than a class. Everything below is importable; items prefixed with `_` are internal and omitted.

### Entry points

#### `main(data_dir, out_dir)`

Top-level driver with the full fallback strategy.

| | |
|---|---|
| **Parameters** | `data_dir` *(str)*: directory containing `requests.log`. `out_dir` *(str)*: output directory (created if needed). |
| **Returns** | `None`. Writes `<out_dir>/summary.txt`. |
| **Raises** | `FileNotFoundError` if `requests.log` is missing. `ValueError` if the reference path hits a malformed line (wrong field count, bad timestamp, non-integer latency or status). `OSError` for other I/O problems. |
| **Side effects** | Calls `gc.disable()` and does not re-enable it. May fork child processes. |

Strategy:

1. Try `fast_main` (parallel).
2. On `Fallback` (input not what the fast path assumes) → `reference_main`.
3. On any other `Exception` (fork failure, out of memory, worker crash, etc.) → retry `fast_main(single=True)`, then → `reference_main` if that also fails.

#### `fast_main(data_dir, out_dir, single=False)`

Optimised implementation. Parses in parallel, then computes percentiles in parallel.

| | |
|---|---|
| **Parameters** | `data_dir` *(str)*, `out_dir` *(str)*, `single` *(bool, default `False`)*: if `True`, use one process and no forking. |
| **Returns** | `None`. Writes `summary.txt`. |
| **Raises** | `Fallback` if the input violates fast-path assumptions. `WorkerFailed` if a child process dies for an unrelated reason. `FileNotFoundError` / `OSError` on I/O errors. |

Parallelism is only used when the file is at least 2 MiB (below that, one process is used). Phase 2 (ranking) is also single-process when the total latency data is under about 1 MiB of buffers.

#### `reference_main(data_dir, out_dir)`

Straightforward single-process implementation using `datetime.strptime` and `sorted`. Defines the correct output; used as the fallback.

| | |
|---|---|
| **Parameters** | `data_dir` *(str)*, `out_dir` *(str)* |
| **Returns** | `None`. Writes `summary.txt` (UTF-8, `\n` newlines). |
| **Raises** | `ValueError` on malformed lines (wrong field count, timestamp not matching `%Y-%m-%dT%H:%M:%S.%f%z`, non-integer status or latency). `FileNotFoundError` / `OSError`. |

### Parsing

#### `parse_chunk(buf, acc)`

Parse a `bytes` buffer containing whole lines and merge results into `acc`.

| | |
|---|---|
| **Parameters** | `buf` *(bytes)*: complete lines. `acc` *(dict)*: accumulator mapping `(hour_bytes, endpoint_bytes)` → `[error_count, array('I')]`; mutated in place. |
| **Returns** | `None` |
| **Raises** | `Fallback` if any line does not have exactly 5 tokens, spot-checked timestamps are malformed, or a latency is not a valid unsigned 32-bit integer. |

#### `process_range(fd, a, b, acc)`

Read bytes `[a, b)` of an open file descriptor in cache-sized chunks (cut at line boundaries) and call `parse_chunk` on each.

| | |
|---|---|
| **Parameters** | `fd` *(int)*: open file descriptor. `a`, `b` *(int)*: byte offsets, both on line boundaries. `acc` *(dict)*: accumulator as above. |
| **Returns** | `None` |
| **Raises** | `Fallback` if the file shrinks during the read or a single line exceeds the remaining range without a newline. |

#### `line_bounds(fd, size, ntasks)`

Compute `ntasks + 1` increasing byte offsets that split the file into roughly equal ranges, each inner offset aligned to the start of a line.

| | |
|---|---|
| **Returns** | `list[int]` of length `ntasks + 1`, starting at `0` and ending at `size`. |

### Percentiles

#### `ranks_of(pieces)`

| | |
|---|---|
| **Parameters** | `pieces` *(list of bytes-like)*: uint32 buffers for one *(hour, endpoint)* group. |
| **Returns** | `tuple (n, p50, p95, p99)` of ints. |

Uses one of three methods depending on size and value diversity: a plain sort (small groups), a counting histogram (many repeated values, up to 2,000,000 distinct), or a piecewise sort with binary search on value (very large groups).

### Process helpers

| Function | Description |
|---|---|
| `cpu_budget()` | Cached worker count: `A_INGEST_WORKERS` if set, otherwise usable CPUs capped by any cgroup quota; always ≥ 1. |
| `spawn(fn)` | Fork a child that runs `fn()` and writes the returned `bytes` to a pipe. Returns `(pid, read_fd)`. |
| `collect(children)` | Read and join all children. Returns `list[bytes]`. Raises `Fallback` if a child signalled fallback (exit code 3), `WorkerFailed` if any child failed otherwise. |

### Exceptions

| Class | Meaning |
|---|---|
| `Fallback(Exception)` | Input is not what the fast path assumes; use the reference algorithm. |
| `WorkerFailed(Exception)` | A child process died for a reason unrelated to the data (e.g. out of memory). |

### Constants

| Name | Value | Purpose |
|---|---|---|
| `INPUT_NAME` | `"requests.log"` | Input filename inside `data_dir` |
| `OUTPUT_NAME` | `"summary.txt"` | Output filename inside `out_dir` |
| `TIMESTAMP_FORMAT` | `"%Y-%m-%dT%H:%M:%S.%f%z"` | Used by the reference path only |
| `CHUNK` | `256 KiB` (env `A_INGEST_CHUNK`) | Bytes per parse chunk |
| `PIECE` | `1 << 20` | Max elements sorted at once within a group |
| `MAX_TASKS` | `8192` | Upper bound on parallel tasks (queue must fit a 64 KiB pipe buffer) |

---

## 5. Code Examples

### Example 1: Run end to end from Python

Self-contained and runnable (place next to `data_processor.py`):

```python
import os
import tempfile
import data_processor

with tempfile.TemporaryDirectory() as tmp:
    data_dir = os.path.join(tmp, "data")
    out_dir = os.path.join(tmp, "out")
    os.makedirs(data_dir)

    lines = [
        "2024-05-01T10:00:01.123456Z GET /api/users 200 1500",
        "2024-05-01T10:15:42.000001Z GET /api/users 500 9000",
        "2024-05-01T10:30:10.500000Z POST /api/orders 201 4200",
        "2024-05-01T11:02:03.250000Z GET /api/users 503 7000",
        "2024-05-01T11:45:59.999999Z GET /api/users 200 1200",
    ]
    with open(os.path.join(data_dir, "requests.log"), "w") as f:
        f.write("\n".join(lines) + "\n")

    data_processor.main(data_dir, out_dir)

    with open(os.path.join(out_dir, "summary.txt")) as f:
        print(f.read(), end="")
```

Output:

```
2024-05-01T10:00:00Z /api/orders 1 4200 4200 4200 0 0
2024-05-01T10:00:00Z /api/users 2 1500 9000 9000 1 500000
2024-05-01T11:00:00Z /api/users 2 1200 7000 7000 1 500000
```

### Example 2: Command line

```bash
python data_processor.py --data ./logs --out ./reports
cat ./reports/summary.txt
```

### Example 3: Control parallelism

```bash
# Use exactly 4 workers
A_INGEST_WORKERS=4 python data_processor.py --data ./logs --out ./reports

# Force single-process mode
A_INGEST_WORKERS=1 python data_processor.py --data ./logs --out ./reports
```

### Example 4: Parse a summary line back into fields

```python
def parse_summary_line(line):
    hour, endpoint, count, p50, p95, p99, errors, ppm = line.split()
    return {
        "hour": hour,
        "endpoint": endpoint,
        "count": int(count),
        "p50_us": int(p50),
        "p95_us": int(p95),
        "p99_us": int(p99),
        "errors": int(errors),
        "errors_ppm": int(ppm),
    }

with open("reports/summary.txt") as f:
    rows = [parse_summary_line(line) for line in f]
```

### Example 5: Call the percentile helper directly

```python
from array import array
import data_processor

latencies = array("I", [10, 20, 30, 40, 50])
print(data_processor.ranks_of([latencies.tobytes()]))
# (5, 30, 50, 50)  ->  n, p50, p95, p99
```

---

## 6. Error Handling & Edge Cases

### Empty input

An empty `requests.log` (0 bytes) is valid and produces an empty `summary.txt`. The output directory is still created.

### Malformed or unexpected data

| Situation | Behaviour |
|---|---|
| A line does not have exactly 5 whitespace-separated tokens | Fast path raises `Fallback`; `main` reruns with the reference path, which raises `ValueError` (`not enough values to unpack` / `too many values`). |
| Timestamp not in the expected shape | Same: fast path falls back; reference raises `ValueError` if it also fails `strptime`. |
| Timestamp missing fractional seconds (e.g. `2024-05-01T10:00:01+00:00`) | Reference path rejects it with `ValueError`; the fraction is required. |
| Timestamp with a non-`Z` offset, valid fraction | Fast path falls back; reference accepts it and labels the hour with `Z` in the output (the offset is not converted). |
| Latency not an integer, negative, or larger than an unsigned 32-bit integer | Fast path falls back; reference raises `ValueError` for non-integers. |
| Status code not 3 digits | Handled exactly via `int()`; any value 500–599 counts as an error. |
| Lines out of time order | Supported; the parser detects ordering per chunk and uses the slower general grouping path when needed. |

Malformed input is **not silently skipped**: if the reference path cannot parse a line, `main` raises and no summary is written.

### Process and resource failures

- If a worker hits a data problem, it signals `Fallback` (exit code 3) and the whole run falls back to the reference algorithm.
- If a worker dies for any other reason (e.g. killed for memory), `WorkerFailed` is raised internally; `main` retries single-process, then falls back to the reference algorithm.
- If the file changes size while being read, `Fallback` is raised.

### File I/O

| Situation | Behaviour |
|---|---|
| `requests.log` missing | `FileNotFoundError` propagates from `main`. |
| `out_dir` does not exist | Created with `os.makedirs(..., exist_ok=True)`. |
| `out_dir` not writable | `OSError` / `PermissionError` propagates. |
| Existing `summary.txt` | Overwritten. |
| Unrecognised CLI argument or missing option value | Exits with status 1 and an `error:` message. |

### Other things to know

- **Memory:** all latencies are held in memory as 4 bytes each, so a log with *N* requests needs on the order of `4 × N` bytes for latency data (plus transient overhead while parsing and merging).
- **Garbage collection:** `main()` disables the GC and does not restore it. Re-enable with `gc.enable()` if you embed this in a longer-running program.
- **Endpoints** are treated as opaque byte strings by the fast path and are never decoded, so non-UTF-8 endpoint bytes round-trip unchanged on that path.
- **Determinism:** output ordering is fixed (hour, then endpoint) and independent of the number of workers.

---

## 7. How It Works

**Phase 1: parse (parallel)**

1. The file is divided into up to 8192 line-aligned byte ranges (`line_bounds`).
2. Worker processes pull task indices from a shared pipe, so faster workers take more work.
3. Each worker reads chunks with `os.pread`, splits the whole chunk into tokens at once, and slices out the timestamp / endpoint / status / latency columns.
4. Lines are grouped by *(hour, endpoint)*. When a chunk is time-ordered, hour boundaries are found by bisection; otherwise a general per-line grouping is used.
5. Latencies are stored in `array('I')`; 5xx lines are counted per group.
6. Workers return their partial results to the parent via `marshal` over pipes, and the parent merges them.

**Phase 2: percentiles (parallel)**

Groups are assigned to workers by largest-first load balancing, and `ranks_of` computes `n`, p50, p95, and p99 for each.

**Output**

Results are formatted per group and written to `summary.txt` in one write.

**Fallback**

Any deviation from the fast path's assumptions, or any infrastructure failure, routes to `reference_main`, which is the byte-for-byte definition of correct output.
