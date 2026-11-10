# fauxflake

End-to-end CI for dbt on Snowflake that never touches Snowflake.

Your unmodified dbt project runs through the real `dbt-snowflake` adapter against a local
Snowflake endpoint backed by DuckDB. The source tables are synthetic: generated from a small
spec of aggregate statistics describing what production looks like (volume, grain, skew,
duplicates, late arrival, row lifecycles). Then the clock moves forward day by day, so
incremental models and snapshots are tested the way production exercises them.

No credentials in CI, no rows leave the warehouse, no warehouse spins up.

How close is it to the real thing? Fourteen third-party dbt packages (973 models) were built
on this emulator and on real Snowflake and compared row for row: 944 models are identical,
22 differ only because they read the clock, and 7 differ in an order Snowflake leaves
undefined or hold values made with `RANDOM()`. [Details and the other measurements](#is-the-emulator-right).

| | |
| --- | --- |
| `fauxflake init` | write the data spec from the dbt project itself |
| `fauxflake dev` | rebuild on save, show what changed against your last commit |
| `fauxflake build` | generate sources and `dbt build` |
| `fauxflake simulate` | let days pass, then compare incrementals to a full refresh |
| `fauxflake scenarios` | replay bad days and report silent data corruption |
| `fauxflake diff` | what a branch changes in the output, as a PR comment |
| `fauxflake check` | the spec still covers every column and function the project uses |
| `fauxflake query` | Snowflake SQL against the warehouse a run left behind |
| `fauxflake profile` | measure production's shape from Snowflake, aggregates only |
| `fauxflake parity` | prove the emulator and real Snowflake give identical rows |
| `fauxflake conformance` | score the emulator against answers recorded from Snowflake |

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

## On your project

```sh
uv tool install git+https://github.com/mkebiclioglu/fauxflake@v0.2.0
cd your-dbt-project
fauxflake init      # writes fauxflake.yml from the project itself; no Snowflake access
fauxflake build
```

`init` reads what the project already declares: source columns from how models select them
(following the usual `source` → `renamed` staging pattern and `ref`s between models), types
from casts and naming, the grain from `unique` tests, references from `relationships` tests,
categories from `accepted_values`, the load timestamp from freshness config. Tests written on
marts are traced back through staging to the raw table. It prints every assumption it made.

It also reads the shape of the data out of the SQL, so that models have rows to work on:

| the SQL says | so the data has |
| --- | --- |
| `user:login::string` | a JSON column with a `login` field |
| `to_date(x, 'YYYY/MM/DD')` | text dates in that format |
| `where state = 'open'` | `open` among the values |
| `where name like 'tap-%'` | names that fit the pattern |
| `a.account_id = b.id` | `account_id` drawn from `b`'s keys, so the join matches |
| `a.region = b.region_code` | both drawn from one shared set of values |
| `sum(x)`, `x * 2` | a number |
| columns lined up by a `UNION` | one type |
| `author_id` next to `author_username` | the name follows the id in every table that has both |

Volumes and distributions start as placeholders. `fauxflake profile` replaces them with
measurements once it has a read-only Snowflake connection.

dbt packages are installed on first use. Seeds, snapshots, ephemeral models, incremental
strategies, `dbt_utils`, `persist_docs`, `grants`, query tags, secure views and dynamic tables
(rebuilt on every run) are covered by this repo's tests. Three things a real project may need:

- **Functions that exist only in your account.** `init` finds them and writes a placeholder
  for each that returns NULL, so the project builds on the first try. Replace the ones that
  matter with DuckDB SQL; `fauxflake check` lists any that are still missing.
  ```yaml
  udfs:
    fiscal_year: "(d) as year(d + interval 11 month)"
  ```
- **Logic that branches on the target.** The dbt target is called `fauxflake`. If the project
  tests `target.name`, pick the name it expects: `dbt: {target: ci}` in the spec.
- **Models that cannot run locally** (external functions, Snowpark, streams). Tag them and
  pass dbt's own selector through: `fauxflake build --exclude tag:snowflake_only`.

When a model comes out empty or wrong, look at it. Keep the warehouse with `--db-path` and
ask it questions in Snowflake SQL:

```sh
fauxflake build --db-path .warehouse
fauxflake query --db-path .warehouse "select status, count(*) from fct_orders group by 1"
```

## While you type

```sh
fauxflake dev
```

```
generated 1,872,531 source rows
ok     everything  (1.1s)
watching for changes (Ctrl-C to stop)
ok     stg_orders.sql  (0.5s)
       fct_orders: 3,715 of 79,967 rows differ from HEAD  [NET_AMOUNT 3,715]
       stg_orders: 3,715 of 79,967 rows differ from HEAD
```

Save a file and the edited models and their children are rebuilt, followed by how the output
now differs from your last commit. Offline, no warehouse, about half a second.

## What did my change do?

```sh
fauxflake diff --base origin/main
```

The base branch and your working tree are built on exactly the same synthetic rows, so any
difference in the output is caused by the code change and nothing else. Changing one
expression in `stg_orders` gives, in 7 seconds:

| model | rows | changed rows | columns |
| --- | --- | --- | --- |
| `fct_orders` | 79,967 | 3,715 (4.6%) | `NET_AMOUNT` 3,715 |
| `stg_orders` | 79,967 | 3,715 (4.6%) (no unique key: counted as rows present in only one version) |  |

3 other models unchanged.

The output is Markdown, ready to post as a pull request comment. Models with a `unique` test
get per-column change counts.

## Bad days

Production-shaped data tests the average day. Bugs live in the others.

```sh
fauxflake scenarios
```

```
scenario          verdict          detail
late-spike        SILENT DRIFT     wrong data, no test failed: fct_daily_events
duplicate-storm   survived         builds passed, incrementals match a full refresh
null-surge        caught           dbt build failed on 6 of 7 days; drift in fct_daily_events
volume-spike      survived         builds passed, incrementals match a full refresh
empty-day         survived         builds passed, incrementals match a full refresh
loader-outage     survived         builds passed, incrementals match a full refresh
new-column        survived         builds passed, incrementals match a full refresh
```

Each scenario bends two days of the synthetic history and replays a week of runs. The
verdict is one of: **survived**, **caught** (a dbt test failed, so someone would have been
paged), or **silent drift** (every test passed and the data is wrong; the command exits 1).

That is the example project's real output, and the example is left that way on purpose. Its
three-day late window does not cover a backlog. And the null surge exposed a bug nobody
planted: `fct_daily_events` uses `delete+insert` keyed on `event_type`, a `NULL` key never
matches its own delete, so those rows duplicate on every run.

## In CI

```yaml
- uses: actions/checkout@v7
  with: {fetch-depth: 0}
- uses: mkebiclioglu/fauxflake@v0.2.0
  with: {command: build}
- uses: mkebiclioglu/fauxflake@v0.2.0
  with: {command: simulate, args: --days 14}
```

[`docs/workflows/dbt-ci.yml`](docs/workflows/dbt-ci.yml) is a complete pull request workflow:
spec check, build, simulation, scenarios, and the data diff posted as a comment.
[`docs/workflows/refresh-profile.yml`](docs/workflows/refresh-profile.yml) re-measures
production weekly and opens a pull request with the changes.

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

1. **Profile.** The only step that talks to Snowflake. A few aggregate queries per table over
   a trailing window, optionally block-sampled. Output is a YAML diff.
2. **Generate.** Every value is a pure function of `(seed, table, column, row number)`,
   computed in vectorised DuckDB SQL.
3. **Emulate.** [fakesnow](https://github.com/tekumara/fakesnow) serves the Snowflake wire
   protocol and transpiles Snowflake SQL to DuckDB with sqlglot. fauxflake runs it in-process,
   shares its DuckDB instance with the generator, and adds a semantics layer for the places
   where the two engines give different answers.

## The five questions this started from

**Getting data out of Snowflake is expensive. Can we generate it instead?**
Yes, and only statistics are needed. Per table the profiler reads daily row counts, quantiles
of load lateness and of numeric and date columns, null rates, the duplicate rate, how
concentrated each reference is, and frequencies of low-cardinality values. `max()` and row
counts come from metadata; the rest is a scan of a trailing window that `--sample-pct`
shrinks further. Nothing is unloaded.

**How would terabytes be generated, and look real?**
For pull requests, do not generate terabytes. What breaks a dbt change is shape, not bytes:
grain, fan-out, duplicates, nulls, skew, late and out-of-order rows, state changes.
`--scale 0.001` shrinks every table by the same factor, so those ratios survive while a build
takes seconds. When you do want volume, `--db-path` keeps the data in DuckDB files rather
than memory: 281 million rows generate in 102 seconds (2.7M rows/s) on an 8 GB laptop with
DuckDB capped at 4 GB.

| Property | How it is reproduced |
| --- | --- |
| Volume over time | level, weekly seasonality, yearly growth, noise, fitted from daily counts |
| Grain | declared key; uniqueness by construction |
| Relationships | a child draws a parent row *number* and applies the parent's key function, so foreign keys resolve without joining |
| Skew ("a few customers place most orders") | measured in production, then the generator is tuned locally until it reproduces the same concentration |
| Numeric and date columns | inverse-CDF sampling through measured quantiles |
| Categories | measured frequencies |
| Mutable rows | lifecycles (`placed → shipped → delivered → returned`) with transition probabilities and delays, as a change log or as an upsert table (`mode: upsert`) |
| Loader behaviour | at-least-once duplicates, redelivered seconds to minutes later |
| PII | never read. High-cardinality strings are invented from templates |

**How do we simulate days and months passing?**
There is no stored state to evolve. The whole history is a deterministic "universe", and the
warehouse at simulated time `T` is `universe WHERE _loaded_at <= T`. Advancing the clock is an
`INSERT` of the rows with `T_prev < _loaded_at <= T`. Stepping there in any number of hops
gives byte-identical tables to generating at `T` directly, which is tested, with and without
scenarios. The emulator answers `current_date` and `current_timestamp` with the simulated
time.

**How do we account for late-arriving data?**
It falls out of the same rule. Each row gets an event time and a load time drawn from the
measured lateness distribution. A row is late when its event day is old but its load time
lands in today's window, so each advance delivers yesterday's rows plus a tail from earlier
days. Order versions can arrive out of order, and a dimension row can arrive after the fact
that references it. `simulate` then checks the consequences: each incremental model equals a
full refresh, and each snapshot still has one current row per key and no overlapping
versions.

**How do we keep it up to date with prod?**
The weekly profile workflow opens a pull request against the spec. The diff is the drift
report: a new column, a category whose share moved, volume up 30%, a lateness tail that
doubled. `fauxflake check` fails CI when models read a column the spec does not generate.

## Is the emulator right?

Measured, not assumed, in four ways. Everything below was run; the commands are in this repo.

**1. Answers recorded from Snowflake.** [`conformance/cases.yml`](conformance/cases.yml) holds
287 small queries covering the SQL analytics code leans on: number scale and rounding, null
ordering, implicit conversions, dates and time zones, strings and regular expressions,
semi-structured data, arrays, window functions, pivots, set operations, name resolution.
`fauxflake conformance --connection <name>` runs them on real Snowflake once and stores the
answers in `golden.json`. CI replays them against the emulator with no Snowflake access.

Current score: **287 of 287 match.** A case that stops matching fails CI.

**2. Other people's projects, row for row against Snowflake.** Thirteen Fivetran packages and
Snowplow's unified package ship integration tests with seed data. Each was built twice, on the emulator and on a real
Snowflake account, and every model compared row for row
(`scripts/selftest.py <project> --snowflake <connection>`):

| package | models | identical | differ only by the clock | other |
| --- | --- | --- | --- | --- |
| HubSpot | 167 | 167 | | |
| Shopify | 211 | 207 | | 4 |
| QuickBooks | 96 | 95 | | 1 |
| NetSuite | 93 | 93 | | |
| Zendesk | 76 | 71 | 5 | |
| Stripe | 63 | 63 | | |
| Snowplow unified | 51 | 42 | 7 | 2 |
| Jira | 48 | 44 | 4 | |
| Intercom | 42 | 42 | | |
| Recharge | 40 | 40 | | |
| GitHub | 37 | 31 | 6 | |
| Salesforce | 24 | 24 | | |
| Klaviyo | 18 | 18 | | |
| Mixpanel | 7 | 7 | | |

"Differ only by the clock" are models that compute time since `current_timestamp`: the two
builds ran minutes apart. The "other" models differ in the order of a `LISTAGG` with no
`ORDER BY` (Shopify), in a running total over rows that tie in its `ORDER BY` (QuickBooks), or
hold test values made with `RANDOM()` (Snowplow). No model differs for any other reason, and
on the emulator each package also passes all of its own tests.

It did not start that way. This comparison is what found that `persist_docs` broke every model,
that a flattened value never equalled the text it holds, that arrays built in a `CASE` could
not be unioned, that `v['a'][0]['b']` and `v[0].name` read the wrong element, and that
`ARRAY_AGG ... WITHIN GROUP` kept NULLs. All fixed, each with a
conformance case.

**3. Packages that test themselves.** These bring inputs, expected outputs and the tests that
compare them. On the emulator:

| package | nodes | pass |
| --- | --- | --- |
| dbt-utils integration tests | 237 | 237 |
| dbt-expectations integration tests | 169 | 169 |
| dbt-date integration tests | 73 | 73 |
| dbt_artifacts integration project | 136 | 136 |
| Snowplow unified integration tests | 234 | 234 |

**4. Projects with no data to borrow.** Two measurements.

*Zero-config, end to end.* On Meltano's public project (207 nodes, 11 raw tables, JSON-heavy
sources), `fauxflake init` followed by `fauxflake build`, with no hand edits: every model
builds and **206 of 207 nodes pass**. The one failure is a uniqueness test tripped by invented
values. The first attempt passed 27; each step up was a fix that now ships.

*Breadth.* [`scripts/compat.py`](scripts/compat.py) renders projects with the real adapter
and runs every model through the emulator on empty stub tables whose columns it has to guess:

| project | models | run end to end | emulator failure | blocked by guessed stub columns, or undecided |
| --- | --- | --- | --- | --- |
| GitLab | 2,679 | 2,266 (84.6%) | 14 (0.5%) | 399 (14.9%) |
| Mattermost analytics | 194 | 182 (93.8%) | 6 (3.1%) | 6 (3.1%) |
| Flipside near | 63 | 60 (95.2%) | 2 (3.2%) | 1 (1.6%) |
| Snowflake monitoring | 25 | 21 (84.0%) | 1 (4.0%) | 3 (12.0%) |

The emulator failures that remain are functions that exist only in those accounts (which
`init` would give placeholders), a lambda containing a subquery, and a table function over
external files. Tables are empty in this measurement, so it shows that SQL runs, not that
answers are right; the three measurements above cover that.

**On your own project:** `fauxflake parity --connection <name>` seeds the same synthetic rows
into your account, builds on both engines through simulated days, and compares every model.
On the example it costs a fraction of a credit:

```
same  fct_daily_events   emulator=248 snowflake=248 only_emulator=0 only_snowflake=0
same  fct_orders         emulator=82,512 snowflake=82,512 only_emulator=0 only_snowflake=0
same  stg_events         emulator=1,699,803 snowflake=1,699,803 only_emulator=0 only_snowflake=0
```

The fixes live in [`semantics.py`](src/fauxflake/semantics.py). Some of what these
measurements found, all of it silent before:

- integer division and `AVG` return 6 decimals, a windowed `AVG` 3, and `/ 0` is an error;
- `1.50` is `NUMBER(2,1)`, floats print as `100` not `100.0`, `SUM` stays exact;
- a timestamp cast to text always shows milliseconds, which changes every surrogate key built
  from one;
- a float cast to an integer rounds half away from zero (`2.5` is `3`);
- `DATE_TRUNC` and `date + interval '1 month'` keep `DATE`;
- `ARRAY_AGG` over no rows is `[]`, not NULL;
- `FLATTEN(..., OUTER => TRUE)` was dropping the outer rows, and `value::string` kept its
  JSON quotes;
- NULL timestamps reached the client as `1970-01-01`;
- `x / 2 as r, r * 2` could not reuse the alias;
- text columns convert where Snowflake converts them: `SUM(text)`, `text * 2`,
  `text_date > current_date`, `DATEADD(day, 1, text)`.

## Security

- CI needs no Snowflake credentials. The dbt profile fauxflake generates points at `127.0.0.1`.
- The spec holds aggregates only. A categorical value is recorded only if at least
  `--k-anonymity` rows share it (default 50); rarer values are folded into `__other__`.
  Columns with more than `--max-categories` distinct values are not read at all.
- The profiler role needs `SELECT` on raw schemas and an XS warehouse, nothing else.
- The spec is plain text in git, so what left the warehouse is reviewable and auditable.

## The spec

[`example/fauxflake.yml`](example/fauxflake.yml) is a complete one. Column generators:
`sequence`, `hash_id`, `fk`, `categorical`, `dist`, `template`, `lifecycle`, `time_offset`,
`event_time`, `updated_at`, `loaded_at`, `constant`, `object`, `array`, `pool` (values shared
by every column naming the same pool). Any column takes `null_rate` and `type`.
A table takes `volume`, `lateness`, `duplicate_rate`, `intraday`, `mode: upsert` and
`scale: false` (for reference tables that should not shrink). Top-level `udfs` declares
stand-ins for account-specific functions; `dbt: {target, database, env}` sets the dbt target
name, the database dbt builds into and placeholders for environment variables. Exclude a model from the drift comparison with
`meta: {fauxflake_drift: false}`. `fauxflake serve` keeps the emulator up with data loaded,
for running `dbt` yourself.

## Limits

Read these before trusting a green check.

- **The emulator is not Snowflake.** SQL coverage is whatever fakesnow and sqlglot can
  transpile, plus the semantics layer here. Unsupported statements fail immediately with a
  `fauxflake emulator:` message; they do not hang or pass. Out of scope: streams, tasks,
  Snowpark and Python models, JavaScript UDFs (give them SQL stand-ins), external stages and
  `COPY INTO` from cloud storage, masking and row access policies. Keep one cheap stage on real
  Snowflake: `dbt build --empty --select state:modified+` compiles and type-checks every
  changed model against real schemas while scanning zero rows.
- **Things that are accepted and do nothing:** grants, comments from `persist_docs`, query
  tags, clustering keys, warehouse and session settings. A dynamic table is rebuilt in full on
  every run. Qualified temporary tables become regular tables; explicit transactions run as
  autocommit.
- **Known result differences.** `HASH()` returns different numbers (the algorithm is
  Snowflake's own). Results of division and `AVG` carry Snowflake's value and scale but are
  typed `DOUBLE` rather than `NUMBER`. The session time zone is UTC. A `TIMESTAMP_TZ` is an
  instant: it does not remember the offset it was written with. Where Snowflake leaves an
  order undefined (`LISTAGG` without `ORDER BY`, ties in a window's `ORDER BY`), the two
  engines pick differently.
- **It is more lenient in a few places.** `DATE_TRUNC` and `YEAR` accept a text column here
  and are rejected by Snowflake. A green local run does not prove Snowflake accepts the SQL;
  the `--empty` stage above does.
- **It says nothing about Snowflake performance or cost.** Query plans, pruning, spilling and
  warehouse sizing cannot be learned from DuckDB.
- **Fidelity stops at the spec.** Apart from attributes that follow an id and columns that are
  joined, columns are sampled independently, so cross-column correlations are lost. Lifecycles
  are declared by hand. `init` guesses types from names when the SQL gives no evidence. A test
  that encodes a business rule the data cannot know can fail on invented data until the spec
  is taught the rule; exclude it or edit the spec.
- **Scale is relative.** Logic that depends on absolute counts (`having count(*) > 1000`)
  behaves differently at `--scale 0.001`.
- **The profiler has run against one real account**, on synthetic tables. Expect rough edges
  on wide or semi-structured production tables.
- Row values depend on DuckDB's `hash()`, so the DuckDB version is pinned in `uv.lock`. The
  emulator's internals are patched, so its minor version is pinned too.

## Development

```sh
uv sync
uv run pytest    # about a minute; the only network use is installing dbt_utils once
```

Three tools for finding the next gap, in the order they are worth reaching for:

```sh
# 1. a behaviour: add a query to conformance/cases.yml, then record Snowflake's answer
uv run fauxflake conformance --connection <name>

# 2. a package that ships seeds and tests: run it here, and on Snowflake, and compare every model
uv run python scripts/selftest.py path/to/package/integration_tests --snowflake <name>

# 3. any project, with no data: does every model plan and run on empty tables?
uv run python scripts/compat.py render  path/to/project --out rendered/
uv run python scripts/compat.py execute path/to/project rendered/
```

`FAUXFLAKE_SHOW_SQL=1` prints the DuckDB SQL each statement was translated to.

MIT licensed.
