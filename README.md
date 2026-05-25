# RiverWriter / RIVER

One-stop shop for Dukascopy 1-minute FX OHLCV data — assembled per **candidate_C**
substrate shape: fetch → Parquet → DuckDB → GX checkpoint → ATOM-ready bars.

## Architecture (candidate_C)

```
Dukascopy
    ↓
RiverWriter (fetch / aggregate / write)          ← construction-time fast gate (writer.py)
    ↓
Parquet  data/1m/{PAIR}/{PAIR}_{YEAR}.parquet    ← local atomic yearly partitions (gitignored)
    ↓
DuckDB   data/river.duckdb                      ← queryable substrate (views)
    ↓
GX       bar_suite_v1                           ← ingestion-boundary validation ONLY
    ↓
ATOM     (consumes bars via DuckDB views)
```

### Scope discipline (no bloat, no duplication)

| Layer | Module | Role | Authoritative for |
|-------|--------|------|-----------------|
| Fetch | `fetcher.py`, `parallel_runner.py` | Download + write Parquet | — |
| Fast gate | `writer.py` | Stamp `knowledge_time` + `bar_hash`, dedupe, reject bad bars, atomically merge yearly partitions | construction-time |
| Substrate | `substrate.py` | DuckDB views over Parquet | query surface |
| Validation | `gx_validate.py` + `expectations/bar_suite_v1.py` | GX checkpoint | **can ATOM consume?** |
| OHLC SQL | `gx_validate.py` (DuckDB) | Structural bar integrity | part of GX checkpoint |
| Timeliness | `health.py` | Lag / pending hours | operational |
| Gaps | `gap_analysis.py` | Weekend-aware gap detection | operational (NOT GX) |
| Inspect | `inspect.py` | Passive local status for COO / future MCP inspect | visibility |

GX is **ephemeral context** — no Data Docs, no checkpoint store bloat.
GX does **not** validate ATOM primitives (candidate_C `GX_layer_drift` guard).

Parquet files are local runtime state, not Git state. The current writer uses an
atomic yearly merge path rather than immutable one-file-per-day writes: it reads
the affected yearly partition, dedupes by timestamp, rejects bad OHLC rows, then
renames a complete replacement file into place. That is the intended prototype
semantics until/unless RIVER is promoted to immutable daily partitioning.

## Quick Start

```bash
cd ~/RiverWriter
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Full candidate_C sync
python run.py --sync --skip-fetch   # catalog + DuckDB + GX (no download)
python run.py --sync                # + parallel catch-up if behind

# Local contract tests (no network, no production data)
python -m unittest discover -s tests -v
```

## Commands

| Command | Description |
|---------|-------------|
| `python run.py --sync` | Catalog rebuild → DuckDB → health → catch-up → GX |
| `python run.py --sync --skip-fetch` | Catalog + DuckDB + GX only |
| `python run.py --substrate` | DuckDB view status |
| `python run.py --gx-checkpoint` | GX ingestion validation (exit 1 on fail) |
| `python run.py --validate` | Legacy combined report shim: GX + operational gaps |
| `python run.py --health` | Timeliness + GX + gaps |
| `python run.py --parallel --catch-up` | Parallel fetch until current |

## Data Layout

```
data/
├── .gitkeep                      # keeps local runtime dir present after clone
├── 1m/                          # Parquet bars (gitignored)
│   └── EURUSD/EURUSD_2024.parquet
├── river.duckdb                 # DuckDB catalog with views (gitignored)
├── catalog.parquet              # Fetch progress (gitignored)
├── gx_validation_report.json    # Last GX checkpoint (gitignored)
└── validation_report.json       # Combined report shim (gitignored)

expectations/
└── bar_suite_v1.py              # GX suite (versioned, in git)

riverwriter/
├── substrate.py                 # DuckDB layer
├── gx_validate.py               # GX checkpoint runner
├── gap_analysis.py              # Operational gaps (not GX)
├── inspect.py                   # Passive local status surface
└── ...
```

Only `data/.gitkeep` is committed. Production machines rebuild local runtime
state by running the commands above; Parquet, DuckDB, catalogs, reports, raw
`.bi5` files, locks, temp files, and logs are ignored.

## DuckDB Query Examples

```sql
-- Views are registered as bars_eurusd, bars_gbpusd, etc.
SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM bars_eurusd;
SELECT * FROM bars_eurusd WHERE timestamp > '2026-01-01' ORDER BY timestamp DESC LIMIT 10;
```

From Python:

```python
from riverwriter import substrate
conn = substrate.connect()
df = substrate.fetch_df(conn, "EURUSD")
```

Passive status for operators / future MCP `inspect[target=river]`:

```python
from riverwriter.inspect import inspect_river
status = inspect_river(["EURUSD"])
```

## GX Expectation Suite v1

Defined in `expectations/bar_suite_v1.py`:

- Column schema matches `BAR_COLUMNS` (9 columns)
- No nulls on required fields
- Unique timestamps
- `source == "dukascopy"`
- `volume >= 0`
- OHLC integrity via DuckDB SQL (supplementary substrate check)

## Git

Parquet, DuckDB catalog, raw `.bi5`, generated reports, locks, temp files, and
logs are **gitignored**. Tools, expectation suite, tests, README, and `STATUS.md`
go to GitHub.

## Production Machine Wiring

After pulling the repo on the production machine:

```bash
cd ~/RiverWriter
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Recreate local substrate state without committing runtime data.
python run.py --rebuild-catalog
python run.py --substrate
python run.py --gx-checkpoint

# Catch up only when ready to hit Dukascopy.
python run.py --parallel --catch-up
python run.py --health
```

`report_progress.py` is an optional ops sidecar for publishing `STATUS.md`.
It is not part of the RIVER data contract.

## Second Pass Candidates

These are not required for COO to wire the repo locally, but they are worth a
follow-up cleanup before this becomes a ratified RIVER module:

- Deduplicate `runner.py` and `parallel_runner.py`; they currently repeat the
  same fetch → parse → aggregate → write loop with different scheduling.
- Make GX / gap checks incremental or partition-scoped. Full-history pandas
  loads are acceptable for the prototype, but they will get heavy as the local
  Parquet store grows.
- Decide the long-term raw `.bi5` retention policy: keep for re-parse evidence
  or prune after successful Parquet write.
- Revisit atomic yearly merge versus immutable daily partitioning if the final
  RIVER contract requires stronger write-once semantics.
- Keep `validator.py` only while legacy callers need `validation_report.json`;
  otherwise delete the shim and let `gx_validate.py` remain the sole authority.

## Cross-reference

Implements the **RIVER** + **validation** slots from
`~/constellation/future_scope/candidate_C_integrated_mcp_body.md`.
