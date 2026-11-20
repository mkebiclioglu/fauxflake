# fauxflake

Run your dbt-on-Snowflake project on your laptop and in CI, without Snowflake.

The project is used as it is. dbt talks to a local Snowflake endpoint through the real
`dbt-snowflake` adapter; DuckDB does the work underneath. The source tables are generated from
a small spec, so there are no credentials in CI, no rows leave the warehouse and nothing costs
credits.

```sh
uv tool install git+https://github.com/mkebiclioglu/fauxflake
cd your-dbt-project
fauxflake init     # reads the project, writes fauxflake.yml, runs everything until it builds
fauxflake build    # from then on: dbt build, in seconds, offline
```

## The bottom line

**It builds real projects from nothing but their code.** `fauxflake init` was pointed at
public dbt projects with no configuration and no access to their data:

| project | models, seeds and snapshots that build | tests that pass |
| --- | --- | --- |
| [GitLab's data warehouse](https://gitlab.com/gitlab-data/analytics), from a [public mirror](https://github.com/ParryChen/GitlabEDW) (1,096 sources) | 3,046 of 3,046 | 4,337 of 4,337, and 699 listed as expected failures |
| [Mattermost analytics](https://github.com/datoria-dev/mirror-mattermost-data-warehouse) (563 sources) | 274 of 274 | 428 of 428, and 32 listed |
| [Mattermost's older project](https://github.com/datoria-dev/mirror-mattermost-data-warehouse) in the same repository (646 sources) | 249 of 249 | 17 of 17, and 1 listed |
| [Flipside NEAR](https://github.com/FlipsideCrypto/near-models-archive) | 64 of 64 | 448 of 448, and 21 listed |
| [Meltano's own project](https://gitlab.com/meltano/squared) | 46 of 46 | 160 of 160, and 1 listed |
| [dbt-snowflake-monitoring](https://github.com/get-select/dbt-snowflake-monitoring) | 28 of 28 | 4 of 4, and 1 listed |

Tests that state facts about production data ("no gaps between block numbers") cannot hold on
invented data. `init` lists them in the spec under `dbt.expected_failures` instead of hiding
them, and every other test is enforced.

**It gives Snowflake's answers.** Three independent measurements, all reproducible from this repo:

| measured | result |
| --- | --- |
| 942 queries whose answers were recorded on real Snowflake | 920 match; 22 are known gaps, listed in the file |
| 14 third-party packages (973 models) built here and on Snowflake, compared row for row | 944 identical, 22 differ only because they read the clock, 7 differ in an order Snowflake leaves undefined or hold `RANDOM()` values |
| dbt-utils, dbt-expectations, dbt-date, dbt_artifacts, Snowplow and The Tuva Project run against their own test suites | all 2,098 models and tests pass |

**It is fast.** Fifteen builds of the example project over two simulated weeks take under 20
seconds. Generating 281 million rows takes 102 seconds on an 8 GB laptop.

## What you do with it

| when | command |
| --- | --- |
| Starting out | `fauxflake init` writes the spec and fits it until the project builds |
| While editing a model | `fauxflake dev` rebuilds on save and shows what changed against your last commit |
| Before pushing | `fauxflake build` (any dbt selector works: `fauxflake build -s my_model+`) |
| A model came out wrong | `fauxflake build --keep`, then `fauxflake query "select ..."` or `fauxflake query --model my_model` |
| A model came out empty | `fauxflake why my_model` names the join or filter where its rows stop (`--all` for every such model) |
| One model or one test again | `fauxflake dbt test -s my_model` runs any dbt command on the kept warehouse |
| Reviewing a pull request | `fauxflake diff --base origin/main` shows which rows a change alters |
| Incremental models and snapshots | `fauxflake simulate --days 14` lets time pass, then compares with a full refresh |
| Bad days | `fauxflake scenarios` replays late data, duplicates, nulls, spikes and outages |
| The project grew | `fauxflake fit` adds new sources and columns to the spec and keeps your edits |
| Matching production's shape | `fauxflake profile` measures volumes and distributions, aggregates only |

### init: from a dbt project to a build that passes

`init` needs no Snowflake access. It reads what the project already says, then tries it out.

Reading the SQL gives each source's columns and the shape of their values:

| the SQL says | so the data has |
| --- | --- |
| `user:login::string` | a JSON column with a `login` field |
| `to_date(x, 'YYYY/MM/DD')` | text dates in that format |
| `to_timestamp(v:created::string)` and `v:created::int` | an epoch number |
| `where state = 'open'` | `open` among the values |
| `where block_id between 9820000 and 10000000` | numbers in that range |
| `where name like 'tap-%'`, `like any ('tap-%', 'target-%')` | names that fit the patterns |
| `rlike(uri, '^/[1-9]+[.][0-9]+/.*')`, `regexp_substr(status, 'Success')` | text the expression matches |
| `split(filename, '/')[0]::number` | text whose first part is a number |
| `split_part(request, ' ', 1) = 'POST'` | text whose first piece is `POST` |
| `try_parse_json(replace(log, 'EVENT_JSON:')):event` | that mark, then JSON with an `event` key |
| `iff(v:status:Failure is not null, 'Fail', 'Success')` | a `Failure` key in few of the documents |
| `where not is_deleted`, `iff(active, 1, 0)` | a boolean |
| `parse_ip(network, 'INET')` | IP ranges |
| `a.account_id = b.id`, `a.account_id in (select id from b)` | `account_id` drawn from `b`'s keys, so the join matches |
| `where server_id = '93my...'` on a column that refers to another table | a row with that id in the table it refers to |
| `on q.warehouse_id = m.entity_id and q.hour = m.start_time` | a meter: one row an hour for each of a few entities |
| `on i.created_at between r.window_start and r.release_date` | periods that follow one another, so a row falls in one |
| `group by license_id having count(distinct seats) = 1` | one number of seats per license |
| `sum(x)`, `x * 2`, `x > 100` | a number |
| columns lined up by a `UNION` | one type |
| a `unique`, `relationships` or `accepted_values` test | a key, a reference, those values |
| `unique_combination_of_columns`, `expression_is_true: "ended_at > started_at"` | the combination once; the two times in that order |
| a seed joined on two columns | the pairs the seed lists |

Tests on marts are traced back through staging to the raw column. Sources in the `SNOWFLAKE`
database (`ACCOUNT_USAGE`, `ORGANIZATION_USAGE`) get their real columns.

Then it runs the project and learns from what fails:

| the run says | so the spec gets |
| --- | --- |
| a model reads a column of a source that was only ever `select *`-ed | that column |
| `Could not convert string 'amount_3fa2' to DOUBLE` | `amount` as a number |
| `dbt_utils.union_relations` finds no columns in the event tables | the columns the next model reads |
| a model reads its own table, or a sibling's, by a hard-coded name | an empty table of that shape to start from, as production has |
| a model reads a table nobody declared | a generated source |
| a `unique` test fails | a column without repeats |
| a test returns rows from `group by day, kind having count(*) > 1` | one row a day for each kind |
| a test returns rows from `group by subscription_id having count(*) > 2` | at most two rows per subscription |
| a test returns rows from `where started_at > ended_at` | the end after the start |
| a model that keeps a row a day for every group runs out of memory | a handful of groups |
| a test about production data fails | an entry under `dbt.expected_failures` |
| the project creates a function in a hook | nothing: the function is used as written |

It prints what it learned and what still fails, with the error. `fauxflake fit` repeats this
on an existing spec whenever the project changes.

### dev: while you type

```
$ fauxflake dev
generated 1,872,531 source rows
ok     everything  (1.1s)
watching for changes (Ctrl-C to stop)
ok     stg_orders.sql  (0.5s)
       fct_orders: 3,715 of 79,967 rows differ from HEAD  [NET_AMOUNT 3,715]
       stg_orders: 3,715 of 79,967 rows differ from HEAD
```

Save a file and the edited models and their children are rebuilt, followed by how the output
now differs from your last commit.

### build and query: look at what happened

```sh
fauxflake build --keep                       # keeps the warehouse in .fauxflake/warehouse
fauxflake query "select status, count(*) from fct_orders group by 1"
fauxflake query --model fct_orders --limit 5 # the model's compiled SQL, as the build ran it
fauxflake query -f scratch.sql
fauxflake dbt run -s fct_orders+             # any dbt command, on the same kept warehouse
```

`query` speaks Snowflake SQL, and its clock stands where the build left it, so
`current_date` means what it meant to the models. The functions the project created during the
build are there too, so views that call them can be read.

### why: a model with no rows

A model that builds empty passes every test on it and proves only that its SQL binds. After `fauxflake build --keep`:

```console
$ fauxflake why silver_social__decoded_actions silver__nft_mints_s3
silver_social__decoded_actions: no rows; rows stop in all_social_receipts: where
  _partition_by_block_number >= 75000000
  reads ANALYTICS.silver_social.receipts: 2520 row(s)
  _partition_by_block_number holds: 9820466, 9820555, 9820569, 9820847, 9821144
silver__nft_mints_s3: no rows; rows stop in mint_transactions: where
  tx_status = 'Success'
  reads ANALYTICS.silver.streamline_transactions_final: 3017 row(s)
  tx_status holds: 'Fail'
```

It walks the model's CTEs on the kept warehouse, finds the first one that is empty although what it reads is not, and in it
the join, `WHERE` condition, `FLATTEN`, `QUALIFY` or `HAVING` that keeps nothing, with a sample of what each column involved
holds. `fauxflake why --all` does this for every model where rows stop (models that are empty only because one before them
is are left out).

`init` asks the same question itself once the project builds: where an empty model's join meets made-up text on one side
and real values on the other (the quarters your own calendar model names), or a filter asks for values by name, the
generated source gets those values and the models that read it are built again.

A `unique` test that fails has the mirror question, and `fauxflake why my_model --unique order_id` answers it: the join
after which a row meets several on the other side (with the values that side holds more than once), the UNION whose
sides share keys, or the relation that already held the key twice.

### diff: what did my change do?

```sh
fauxflake diff --base origin/main
```

The base branch and your working tree are built on the same generated rows, so every
difference in the output comes from the code change:

| model | rows | changed rows | columns |
| --- | --- | --- | --- |
| `fct_orders` | 79,967 | 3,715 (4.6%) | `NET_AMOUNT` 3,715 |
| `stg_orders` | 79,967 | 3,715 (4.6%) |  |

The output is Markdown, ready to post on the pull request.

### parity: the same project on the emulator and on Snowflake

```
$ fauxflake parity --connection prod_readonly_ci --schema ANALYTICS_CI
...
DIFF rows          fct_subscriptions
     emulator=1366 snowflake=1366 only_emulator=12 only_snowflake=12 differ in QUANTITY: emulator '12' snowflake '12.00000'
DIFF undetermined  int_active_days
     the order of rows is not determined: 71 tied group(s) in MAX(is_active) OVER (PARTITION BY user_id ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)

233/259 models built on both engines hold the same rows; 1 difference(s); 9 model(s) whose rows the data does not determine
```

The generated sources are loaded into a schema of your Snowflake account, the project is built on
both engines (every node, also after one fails), and every model is compared row for row. Each
difference names the columns that differ, with a value from each side. Columns that only hold
when the build ran (`current_timestamp()`) are compared as the clock. A model whose result
depends on the order of rows that tie (`ROW_NUMBER()`, `LAG`, a `ROWS` frame, `ARRAY_AGG` without
`WITHIN GROUP`) gives other rows on any engine, Snowflake included; it is reported as
`undetermined`, with the window, rather than as a difference. `--select` narrows the run;
`--report diff.json` keeps every difference. It costs credits: run it when you change engines
or upgrade, not on every pull request.

### simulate: let time pass

```
$ fauxflake simulate --days 14
2025-03-02 00:00  loaded    1,872,531 rows  (0.2s)
2025-03-03 00:00  +     38,016 rows  (generate 0.1s, dbt 0.7s)
...
ok    fct_orders          incremental=98,682 rebuilt=98,682 only_incremental=0 only_rebuilt=0
DRIFT fct_daily_events    incremental=296 rebuilt=296 only_incremental=60 only_rebuilt=60
```

Each day brings that day's rows plus the late tail of earlier days. After the last run every
incremental model is compared with a full refresh and every snapshot is checked for one
current row per key. Tasks that are resumed and have a schedule run at every step. Every dbt
test can pass while an incremental model is wrong; this is the check that finds it.

### scenarios: bad days

```
$ fauxflake scenarios
scenario          verdict          detail
late-spike        SILENT DRIFT     wrong data, no test failed: fct_daily_events
duplicate-storm   survived         builds passed, incrementals match a full refresh
null-surge        caught           dbt build failed on 6 of 7 days; drift in fct_daily_events
volume-spike      survived         builds passed, incrementals match a full refresh
empty-day         survived         builds passed, incrementals match a full refresh
loader-outage     survived         builds passed, incrementals match a full refresh
new-column        survived         builds passed, incrementals match a full refresh
```

**Survived**, **caught** (a test failed, someone would have been paged) or **silent drift**
(every test passed and the data is wrong; exit code 1).

### In CI

```yaml
- uses: actions/checkout@v7
  with: {fetch-depth: 0}
- uses: mkebiclioglu/fauxflake@v0.3.0
  with: {command: build}
- uses: mkebiclioglu/fauxflake@v0.3.0
  with: {command: simulate, args: --days 14}
```

[`docs/workflows/dbt-ci.yml`](docs/workflows/dbt-ci.yml) is a complete pull request workflow:
spec check, build, simulation, scenarios and the data diff as a comment.
[`docs/workflows/refresh-profile.yml`](docs/workflows/refresh-profile.yml) re-measures
production weekly and opens a pull request with what moved.

## What Snowflake it speaks

| | locally |
| --- | --- |
| **SQL** | Snowflake's dialect and its answers: number scale and rounding, NULL ordering, implicit conversions, time zones, `QUALIFY`, `PIVOT`, `LATERAL FLATTEN`, `ASOF JOIN`, `CONNECT BY`, lateral column aliases, `GENERATOR`, `RESULT_SCAN`; unnamed columns are named as Snowflake names them (`COUNT(*)`, `A+1`) |
| **Semi-structured data** | `VARIANT`, `OBJECT`, `ARRAY` and `MAP` with path access, `FLATTEN`, and the object, array and map functions |
| **dbt** | tables, views, incremental models (`merge`, `delete+insert`, `append`), snapshots, seeds, ephemeral models, tests, hooks, packages, `persist_docs`, `grants`, dynamic tables, secure and materialized views |
| **Python models** | Snowpark sends its SQL here, so a Python model runs like a SQL one (`pip install "fauxflake[snowpark]"`); models that only use `session.sql`, `session.table` and pandas need no extra |
| **Functions** | SQL functions and SQL table functions become macros; Python functions run as written; JavaScript functions run in an embedded engine |
| **Procedures** | Snowflake Scripting (variables, `IF`, `CASE`, loops, cursors, `RESULTSET`, `EXECUTE IMMEDIATE`, exception handlers), JavaScript with the `snowflake` API, Python with a session; `CALL`, anonymous blocks and anonymous procedures |
| **Streams** | show the rows inserted and deleted since they were last consumed; a DML statement that reads one consumes it |
| **Tasks** | stored with their schedule and predecessors; `EXECUTE TASK` runs one and its resumed children; `simulate` runs the scheduled ones at every step |
| **Masking and row access policies** | applied wherever the table is read, for the current role (`USE ROLE`, or `dbt: {role: ...}` in the spec) |
| **Loading** | `COPY INTO` from S3, GCS, Azure or HTTPS with `PATTERN` and `FILES`; internal stages and file formats; external table definitions; pipes and alerts, stored and run on demand |
| **Other statements that change things** | sequences, `CLONE` of tables and schemas, `SWAP WITH`, `MERGE`, `INSERT OVERWRITE` |
| **System views and functions** | `INFORMATION_SCHEMA`; `ACCOUNT_USAGE` and `ORGANIZATION_USAGE` as generated sources with their real columns; table functions such as `TASK_HISTORY()` with the right columns and no rows |
| **Transactions** | `BEGIN`, `COMMIT`, `ROLLBACK`: DML and `MERGE` roll back, DDL commits the open transaction, and a statement that fails leaves it open |
| **Tags** | `CREATE TAG` with allowed values, `SET TAG` and `UNSET TAG` on objects and columns, `SHOW TAGS`, `TAG_REFERENCES`, `TAG_REFERENCES_ALL_COLUMNS` and `SYSTEM$GET_TAG`, with inheritance from schema and database |
| **Accepted, with nothing to act on** | grants, comments, clustering keys, warehouses, session settings, `SHOW` for account-level objects |

When the emulator does not know a statement it fails at once with a message that starts with
`fauxflake emulator:`. It does not hang and it does not pass silently.

## Evidence

Everything here was run; the commands are in this repo.

**1. Answers recorded from Snowflake.** [`conformance/cases.yml`](conformance/cases.yml) holds
805 small queries, among them one call of each signature of Snowflake's built-in functions;
38 are marked as known gaps (`BINARY` arguments to text functions, XML, sketch internals). `fauxflake conformance --connection <name>` runs them on real Snowflake
and stores the answers; CI replays them against the emulator with no Snowflake access. A case
that stops matching fails CI.

**2. Packages built twice.** Thirteen Fivetran packages and Snowplow's unified package ship
seed data. Each was built on the emulator and on a real account and every model compared row
for row (`scripts/selftest.py <project> --snowflake <connection>`):

| package | models | identical | clock only | undefined order or random |
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

**3. Packages that test themselves.** dbt-utils (237), dbt-expectations (169), dbt-date (73),
dbt_artifacts (136), Snowplow (234) and The Tuva Project's integration project (1,249: its seeds
load from S3 with `COPY INTO`) pass in full here.

**4. Public projects from a zero-config `init`.** The table at the top. These projects were
chosen because they are large and were written for Snowflake alone: hooks that create
functions and procedures, models that inspect the warehouse while compiling, sources that are
never declared column by column.

## The spec

[`example/fauxflake.yml`](example/fauxflake.yml) is a complete one. What `init` writes is a
starting point that builds; `fauxflake profile` replaces its placeholder volumes and
distributions with measurements from production, reading aggregates only.

| key | meaning |
| --- | --- |
| `start`, `history` | the first day of the generated history and how many days it holds when a run begins (60 unless the dates your SQL compares with need more). In a history of several years, `recent` (days) and `older` (a share) keep the last year at full volume and thin the days before it, so a long history costs what a short one does. `least_rows` is set when the project picks tables by size (`row_count >= 100` in `INFORMATION_SCHEMA.TABLES`): every table holds at least that many rows at any scale |
| `tables` | one entry per source table: `volume`, `lateness`, `duplicate_rate`, `mode: upsert`, and a generator per column. `until_day` / `from_day` give a table rows only while it was in use (a system another took over from); `grain` lists the columns a table of readings has one row a day (or an hour) for |
| column generators | `sequence`, `hash_id`, `fk`, `categorical`, `dist`, `template`, `pool`, `by` (the value goes by another column: `{gen: by, of: FIELD, cases: {amount: {...}}, else: {...}}`), `time_offset`, `event_time`, `updated_at`, `loaded_at`, `lifecycle`, `object`, `array`, `ip`, `constant`; any column takes `null_rate` and `type`. `null_unless` leaves a column null (`empty_unless`: a list empty) in the rows where a rule about the row does not hold (`{{COLUMN}}` stands for another column); `of: 20` on a key makes it name twenty things in turn, for readings taken day after day (the table's `gaps: 0.05` leaves one reading in twenty out); a `pool` takes a `suffix`, or `dotted: true` for values like `7.3.1`; `length: 26` rewrites an id at that length, the same way in every column that holds it; `follows: OTHER` makes a text the same wherever OTHER has the same value; `every: 7` on a boolean flag marks one row in seven, always the same ones, and `spares: FLAG` on an `fk` keeps it away from the parent rows that flag marks; `{org}` in a template or suffix is an organisation (`user_ab12@org7.fauxmail.net`): one of 24 large ones for half of the values, a small one of its own for the rest |
| `existing` | relations the project reads before it builds them; they start each run empty. `keeps` lists columns a snapshot table still has from an earlier version of its model |
| `udfs` | stand-ins, in DuckDB SQL, for functions that exist only in your account |
| `locations` | cloud storage prefixes that `COPY INTO` should read from a local directory instead |
| `dbt` | `database`, `target` (the name `target.name` sees), `role`, `env` (placeholders for environment variables), `expected_failures` |

How the data is made: the whole history is a deterministic function of
`(seed, table, column, row number)`, and the warehouse at simulated time `T` is the rows with
`_loaded_at <= T`. Advancing the clock inserts the rows that arrived since. A row is late when
its event day is old and its load time is today, which is how late-arriving data, duplicates
from at-least-once loaders and out-of-order updates appear without being scripted.

## Limits

- **`HASH()` returns different numbers.** The algorithm is Snowflake's own. The values are
  stable and well distributed, which is what joins and sampling need.
- **Division and `AVG` results are typed `DOUBLE`**, with Snowflake's value and scale.
- **A column made by a query is described as `VARIANT`** where Snowflake says `ARRAY` or `OBJECT`.
- **The session time zone is UTC**, and a `TIMESTAMP_TZ` does not remember its original offset.
- **Where Snowflake leaves an order undefined** (`LISTAGG` without `ORDER BY`, ties in a
  window) the two engines pick differently.
- **A stream reports an update as a delete and an insert** with `METADATA$ISUPDATE` false.
- **Time travel reads current data.** `AT` and `BEFORE` are accepted; there is no history.
- **A transaction that writes to two databases commits at that point** and continues in
  autocommit, with a message. Within one database it behaves as Snowflake's does.
- **External functions, and Java or Scala handlers, return NULL** with a warning: they call
  services or runtimes that are not here. A Python function whose packages are not installed
  does the same. Give them a stand-in under `udfs`.
- **It is more lenient in a few places.** `DATE_TRUNC` and `YEAR` accept a text column here
  and are rejected by Snowflake. A cheap stage on Snowflake closes this:
  `dbt build --empty --select state:modified+` type-checks changed models and scans no rows.
- **It says nothing about performance or cost.** Query plans, pruning and warehouse sizing
  cannot be learned from DuckDB.
- **Generated data knows what the spec knows.** Columns are sampled independently unless the
  spec ties them together, and a business rule the SQL never states is not in the data.

## Security

- CI needs no Snowflake credentials. The dbt profile fauxflake generates points at `127.0.0.1`.
- `init` and `fit` never connect to Snowflake. Only `profile`, `parity` and
  `conformance --connection` do, and only when you run them.
- The spec holds aggregates only. `profile` records a categorical value only if at least
  `--k-anonymity` rows share it (default 50) and never reads high-cardinality columns.
- Python and JavaScript in the project (functions, procedures, Python models) run on the
  machine that runs fauxflake, with its permissions, as dbt macros already do.
- `COPY INTO` from cloud storage reads the URL the project names. A private bucket uses the
  credentials in the environment; `locations` in the spec points it at local files instead.

## Development

```sh
uv sync && uv run pytest
uv run fauxflake conformance                          # replay the recorded answers
uv run fauxflake conformance --connection <name>      # record them again from Snowflake
uv run python scripts/selftest.py <package>/integration_tests --snowflake <name>
```

A difference from Snowflake is fixed in `src/fauxflake/semantics.py` and pinned by a case in
`conformance/cases.yml`. Statements about objects other than tables and views live in
`src/fauxflake/objects.py`, procedure bodies in `scripting.py`, and what `init` learns by
running the project in `fit.py`.
