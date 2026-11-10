# fauxflake

End-to-end CI for dbt on Snowflake that never touches Snowflake.

Your unmodified dbt project runs through the real `dbt-snowflake` adapter against a local
Snowflake endpoint backed by DuckDB. The source tables are synthetic: generated from a small
spec of aggregate statistics that describes what production looks like (volume, grain, skew,
duplicates, late arrival, row lifecycles). Then the clock is moved forward day by day, so
incremental models are tested the way production exercises them.

No credentials in CI, no rows leave the warehouse, no warehouse spins up.

> Status: working scaffold. The example project and everything below runs and is tested in
> this repo's CI. See [Limits](#limits) for what is not there yet.

## Try it

```sh
git clone https://github.com/mkebiclioglu/fauxflake && cd fauxflake
uv sync
cd example
uv run fauxflake build              # generate sources, dbt build
uv run fauxflake simulate --days 14 # 14 simulated days of runs, then compare to a full refresh
```

```
2025-03-02 00:00  loaded    1,872,531 rows  (0.2s)
... dbt build: PASS=11 ...
2025-03-03 00:00  +     38,016 rows  (generate 0.1s, dbt 0.7s)
2025-03-04 00:00  +     32,062 rows  (generate 0.1s, dbt 0.7s)
...
2025-03-16 00:00  +     32,191 rows  (generate 0.1s, dbt 0.8s)
rebuilding from scratch to compare ...

ok    fct_orders          incremental=98,682 rebuilt=98,682 only_incremental=0 only_rebuilt=0
ok    fct_daily_events    incremental=296 rebuilt=296 only_incremental=0 only_rebuilt=0
```

Fifteen dbt builds over two simulated weeks take under 20 seconds on a laptop.

Now break something. `fct_daily_events` reprocesses a trailing window because events arrive
late. Remove the window:

```sh
uv run fauxflake simulate --days 14 --vars '{late_window_days: 0}'
```

```
ok    fct_orders          ...
DRIFT fct_daily_events    incremental=296 rebuilt=296 only_incremental=60 only_rebuilt=60
Incremental models differ from a full refresh after simulated time passed.
```

Every dbt test still passes in that run. The model is simply wrong, and on Snowflake you would
find out weeks later from a dashboard.

## What did my change do?

```sh
uv run fauxflake diff --base origin/main
```

The base branch and your working tree are built on exactly the same synthetic rows, so any
difference in the output is caused by the code change and nothing else. Changing one
expression in `stg_orders` gives, in 7 seconds:

| model | rows | changed rows | columns |
| --- | --- | --- | --- |
| `fct_orders` | 79,967 | 3,715 (4.6%) | `NET_AMOUNT` 3,715 |
| `stg_orders` | 79,967 | 3,715 (4.6%) (no unique key: counted as rows present in only one version) |  |

3 other models unchanged.

The output is Markdown; [`docs/workflows/dbt-ci.yml`](docs/workflows/dbt-ci.yml) posts it as
a pull request comment. Models with a `unique` test get per-column change counts.

## How it works

```
  Snowflake (prod)                      your repo                          CI runner / laptop
 ┌────────────────┐  aggregates only  ┌───────────────┐              ┌──────────────────────────────┐
 │ raw tables     │ ────────────────► │ fauxflake.yml │ ───────────► │ generator (DuckDB SQL)       │
 │                │  weekly, as a PR  │ (the spec)    │              │   deterministic, clock-aware │
 └────────────────┘                   │               │              │              │               │
                                      │ dbt project   │ ───────────► │ dbt-snowflake ──► emulator   │
                                      │ (untouched)   │              │   Snowflake protocol, DuckDB │
                                      └───────────────┘              └──────────────────────────────┘
```

Three parts:

1. **Profile** (`fauxflake profile`). The only step that talks to Snowflake. A few aggregate
   queries per table over a trailing window, optionally block-sampled. Output is a YAML diff.
2. **Generate.** Every value is a pure function of `(seed, table, column, row number)`,
   computed in vectorised DuckDB SQL.
3. **Emulate.** [fakesnow](https://github.com/tekumara/fakesnow) serves the Snowflake wire
   protocol and transpiles Snowflake SQL to DuckDB with sqlglot. fauxflake runs it in-process,
   shares its DuckDB instance with the generator, and patches the gaps dbt hits.

## The five questions

**Getting data out of Snowflake is expensive. Can we generate it instead?**
Yes, and only statistics are needed to do it. Per table the profiler reads daily row counts,
quantiles of load lateness, quantiles of numeric columns, null rates, and frequencies of
low-cardinality values. `max()` and row counts come from metadata; the rest is one scan of a
trailing window that `--sample-pct` shrinks further. Nothing is unloaded, so there is no
egress and no storage of extracts.

**How would terabytes be generated, and look real?**
Mostly you should not generate terabytes. What breaks a dbt change is shape, not bytes: grain,
fan-out, duplicates, nulls, skew, late and out-of-order rows, state changes. `--scale 0.001`
shrinks every table by the same factor, so those ratios survive while a build takes seconds.
"Looks real" comes from:

| Property | How it is reproduced |
| --- | --- |
| Volume over time | level, weekly seasonality, yearly growth, noise, fitted from daily counts |
| Grain | declared key; uniqueness by construction |
| Relationships | a child draws a parent row *number* and applies the parent's key function, so foreign keys resolve without joining, with a tunable skew ("a few customers place most orders") |
| Numeric columns | inverse-CDF sampling through measured quantiles |
| Categories | measured frequencies |
| Mutable rows | lifecycles (`placed → shipped → delivered → returned`) with transition probabilities and delays, emitted as a change log |
| Loader behaviour | at-least-once duplicates, redelivered seconds to minutes later |
| PII | never read. High-cardinality strings are invented from templates |

When you do want volume (a nightly job, a load test of the DuckDB side), raise `--scale`.
Generation runs at about 2.3M rows/s on an 8 GB M2: 94M rows in 41s.

**How do we simulate days and months passing?**
There is no stored state to evolve. The whole history is a deterministic "universe", and the
warehouse at simulated time `T` is `universe WHERE _loaded_at <= T`. Advancing the clock is an
`INSERT` of the rows with `T_prev < _loaded_at <= T`. Stepping there in any number of hops
gives byte-identical tables to generating at `T` directly, which is tested. The emulator also
answers `current_date` and `current_timestamp` with the simulated time.

**How do we account for late-arriving data?**
It falls out of the same rule. Each row gets an event time and a load time drawn from the
measured lateness distribution. A row is late when its event day is old but its load time
lands in today's window, so each advance delivers yesterday's rows plus a tail from earlier
days. Order versions can arrive out of order, and a dimension row can arrive after the fact
that references it. `simulate` then checks the consequence that matters: after N incremental
runs, is each incremental model identical to a full refresh?

**How do we keep it up to date with prod?**
`docs/workflows/refresh-profile.yml` re-profiles weekly and opens a pull request against the
spec. The diff is the drift report: a new column, a category whose share moved, volume up 30%,
a lateness tail that doubled. A human reviews it like code.

## Parity with real Snowflake

Synthetic data is safe to load anywhere, including a real account. `fauxflake parity
--connection <name>` seeds the same rows into Snowflake, builds the project on both engines
through simulated days, and compares every model row for row:

```
same  fct_daily_events   emulator=128 snowflake=128 only_emulator=0 only_snowflake=0
same  fct_orders         emulator=8,500 snowflake=8,500 only_emulator=0 only_snowflake=0
same  stg_customers      emulator=244 snowflake=244 only_emulator=0 only_snowflake=0
same  stg_events         emulator=171,434 snowflake=171,434 only_emulator=0 only_snowflake=0
same  stg_orders         emulator=8,500 snowflake=8,500 only_emulator=0 only_snowflake=0
```

That is the real output for the example project on a trial account, on an XS warehouse, for
a fraction of a credit. The first run did not look like this: every row of
`fct_daily_events` differed, because Snowflake divides integers into `NUMBER(p, 6)`
(`0.090029`) while DuckDB returns a double. That is how the gap was found and fixed.

### Conformance suite

[`conformance/cases.yml`](conformance/cases.yml) holds 52 small queries covering the SQL
analytics code leans on: division and rounding, dates, strings, regular expressions,
semi-structured data, window functions, pivots. `fauxflake conformance --connection <name>`
runs them on real Snowflake once and stores the answers in `golden.json`. CI then replays
them against the emulator with no Snowflake access.

Current score: **48 of 52 match**. The 4 known gaps are marked in the file and listed under
[Limits](#limits). A gap is a strict expected failure, so fixing one forces the marker to be
removed and the list cannot go stale. Fixes live in
[`semantics.py`](src/fauxflake/semantics.py); so far: division scale and divide-by-zero
errors, `AVG` scale, exact (not double) `SUM`, `DATE_TRUNC` keeping `DATE`, `LTRIM`/`RTRIM`
with characters, `RATIO_TO_REPORT`, `CONDITIONAL_TRUE_EVENT`.

## Security

- CI needs no Snowflake credentials. The dbt profile fauxflake generates points at `127.0.0.1`.
- The spec holds aggregates only. A categorical value is recorded only if at least
  `--k-anonymity` rows share it (default 50); rarer values are folded into `__other__`.
  Columns with more than `--max-categories` distinct values are not read at all.
- The profiler role needs `SELECT` on raw schemas and an XS warehouse, nothing else.
- The spec is plain text in git, so what left the warehouse is reviewable and auditable.

## Using it on your project

```sh
cd your-dbt-project
fauxflake init      # writes fauxflake.yml from the project itself; no Snowflake access
fauxflake build     # dbt build on synthetic data
fauxflake simulate  # let time pass, compare incrementals to a full refresh
```

`init` reads what the project already declares: source columns from how models select them
(following the usual `source` → `renamed` staging pattern), types from casts and naming, the
grain from `unique` tests, references from `relationships` tests, categories from
`accepted_values`, the load timestamp from freshness config. Tests written on marts are traced
back through staging to the raw table. It prints every assumption it made.

Volumes and distributions start as placeholders. `fauxflake profile` replaces them with
measurements once you give it a read-only Snowflake connection, and
[`docs/workflows/dbt-ci.yml`](docs/workflows/dbt-ci.yml) puts the whole thing in CI.

Column generators: `sequence`, `hash_id`, `fk`, `categorical`, `dist`, `template`,
`lifecycle`, `time_offset`, `event_time`, `updated_at`, `loaded_at`, `constant`. Any column takes
`null_rate` and `type`. Exclude a model from the drift comparison with
`meta: {fauxflake_drift: false}`.

`fauxflake serve` keeps the emulator up with data loaded, for interactive `dbt` runs.

## Limits

Read these before trusting a green check.

- **The emulator is not Snowflake.** SQL coverage is whatever fakesnow and sqlglot can
  transpile. Unsupported statements fail loudly rather than silently, but a model can pass
  here and fail on Snowflake, or the reverse. Streams, tasks, dynamic tables, Snowpark,
  JavaScript UDFs, grants and masking policies are out of scope. Keep one cheap stage on real
  Snowflake: `dbt build --empty --select state:modified+` compiles and type-checks every
  changed model against real schemas while scanning zero rows.
- **Emulator errors are immediate.** Anything unsupported fails the statement with a
  `fauxflake emulator:` message instead of hanging or passing silently.
- **Three deliberate differences**, all in [`emulator.py`](src/fauxflake/emulator.py):
  qualified temporary tables become regular tables; explicit transactions run as autocommit;
  bare columns in `MERGE ... INSERT VALUES` are qualified with the source alias.
- **Known result differences** (from the conformance suite): `TO_VARCHAR` ignores numeric
  format models; `ARRAY` values cannot be returned to a client (they work inside models);
  `TYPEOF` on a variant reports `JSON`. Division results have Snowflake's value and scale but
  are typed `DOUBLE` rather than `NUMBER`.
- **It says nothing about Snowflake performance or cost.** Query plans, pruning, spilling and
  warehouse sizing cannot be learned from DuckDB.
- **Fidelity stops at the spec.** Columns are sampled independently, so cross-column
  correlations are lost. Foreign-key skew, lifecycles and duplicate rates are declared by
  hand; the profiler does not measure them yet. The profiler does not yet measure date and
  timestamp columns other than the event and load times.
- **Scale is relative.** Logic that depends on absolute counts (`having count(*) > 1000`)
  behaves differently at `--scale 0.001`.
- **The profiler has run against one real account**, on synthetic tables only, and without
  `--sample-pct`. Expect rough edges on wide or semi-structured production tables.
- **Everything is in memory.** Scale is bounded by RAM until the generator writes to disk.
- Row values depend on DuckDB's `hash()`, so the DuckDB version is pinned in `uv.lock`.

## Roadmap

- Profile foreign-key fan-out, duplicate rates and pairwise correlations
- Date, timestamp, array and `VARIANT` column generators
- Upsert-style sources in addition to change logs
- On-disk generation for full-scale runs
- Schema-drift check between the spec and dbt `sources.yml`
- Snapshots in the drift comparison

## Development

```sh
uv sync
uv run pytest
```

MIT licensed.
