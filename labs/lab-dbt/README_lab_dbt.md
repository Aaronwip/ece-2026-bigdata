
# Lab — Bronze to Silver transformation with dbt and DuckDB

Lab instructions: [05.etl-elt-and-medallion-architecture/lab-dbt.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/05.etl-elt-and-medallion-architecture/lab-dbt.md)

- Platform: Onyxia, service `vscode-pyspark`
- Engine: dbt Core 1.12.6 with the `dbt-duckdb` 1.11.0 adapter, DuckDB 1.5.5
- Bronze layer: `s3://$LAB_BUCKET_NAME/bronze/{users,orders}.csv` (50 users, 2,829 orders)
- dbt project: [`lab_medallion/`](../../lab_medallion)

| Section | Status |
| --- | --- |
| dbt project and connection profile | ✅ — [`profiles.yml`](../../lab_medallion/profiles.yml), [`dbt_project.yml`](../../lab_medallion/dbt_project.yml) |
| Bronze sources | ✅ — [`sources.yml`](../../lab_medallion/models/bronze/sources.yml) |
| Silver models | ✅ — [`stg_users.sql`](../../lab_medallion/models/silver/stg_users.sql), [`stg_orders.sql`](../../lab_medallion/models/silver/stg_orders.sql) |
| Generic and singular data tests | ✅ — [`silver.yml`](../../lab_medallion/models/silver/silver.yml), [`tests/`](../../lab_medallion/tests) |
| Full pipeline, lineage, documentation | ✅ |
| Silver export to Parquet | ✅ — `silver/dbt/users.parquet`, `silver/dbt/orders.parquet` |
| Exercises 1 to 5 | ✅ |

Final result of `uv run dbt build`:

```text
WARN 49 assert_users_zip_matches_state
Done. PASS=26 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=27
```

The only warning is intentional: the generator associates states with random zip codes (see exercise 4).

## Environment pitfalls

- After a restart of the Onyxia service, `~/.aws` is lost: `aws ... --profile default` fails with
  `The config profile (default) could not be found`. The profile is recreated with the script of the
  "S3 Profile Details" page. The DuckDB `s3_onyxia_connection` secret must also be recreated, since it contains the
  credentials of the previous session.
- `uv run` only finds the project scripts (`dataset-users`, `dbt`) from inside the project directory, otherwise it fails
  with `No pyproject.toml found` or `Failed to spawn`.
- `profiles.yml` must be created in `lab_medallion/`, next to `dbt_project.yml`. Created at the root of the repository,
  `dbt debug` reports `dbt_project.yml file [ERROR not found]`.
- `uv run duckdb lab_medallion.duckdb -c "..."` is a shell command: pasted inside the DuckDB CLI, it is parsed as SQL
  and nothing is executed. The `$LAB_BUCKET_NAME` variable is also only expanded by the shell.
- `.gitignore` initially contained `init.sqltarget/`: a missing trailing newline merged two rules, so neither
  `init.sql` nor `target/` were ignored. The `*.csv` rule also excluded the `states.csv` seed, fixed with the
  `!lab_medallion/seeds/*.csv` exception.

## Bronze layer

**The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months
later?**

The raw data is the source of truth and stays available exactly as it was received. When a bug is found, the
transformation is fixed and the silver layer is rebuilt from bronze with `dbt build`: the whole history is reprocessed
with the correct logic, without asking the producer to send the data again. If the bronze files had been modified in
place, the original values would be lost and the error could not be corrected. It also keeps an audit trail: any silver
value can be traced back to the raw record it comes from.

**The source reads the whole file on every query. Which other format and layout would you choose if the orders were
ingested every hour for years?**

A columnar format, Parquet, partitioned by date with Hive-style directories, for example
`bronze/orders/year=2026/month=10/day=26/hour=11/part-0.parquet`. Each hourly ingestion writes a new file instead of
rewriting one huge CSV. A query filtered on a period only reads the matching partitions (partition pruning), and only
the needed columns (projection pushdown), with compression and typed values. For many small hourly files, a table
format such as Apache Iceberg adds compaction, schema evolution and transactional writes. On the dbt side, the silver
model would become `incremental` and only process the new partitions.

## Silver layer

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**

A view only stores its SQL definition. Each query on the view executes the definition: DuckDB reads the CSV on S3,
casts, cleans and deduplicates the rows, then applies the query on top of it. Nothing is stored and the result is always
up to date with the source.

A view is preferable when the data is small or cheap to compute, when it is rarely queried, when freshness matters more
than speed, or during development where models change often. A table is preferable when the transformation is
expensive, the source is large or remote (S3), the model is queried often, or when several downstream models depend on
it.

**`UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?**

36 bytes: 32 hexadecimal characters plus 4 hyphens (`bdd640fb-0667-4ad1-9c80-317fa3b1799d`), one byte each in UTF-8,
so 2.25 times more. Casting to `UUID` saves storage and memory, makes comparisons and joins faster, and validates the
format: an invalid identifier makes the cast fail.

## Data tests

### Investigate a failing test

`assert_orders_have_users` passes: every order references an existing user. The check is the same as the
`relationships` generic test of `stg_orders.user_id`, which is the preferred form (see exercise 2).

### Singular test `assert_orders_after_user_birth`

The test returned 288 failing rows, as in the lab. The orders are dated in 2020, while Faker generates birthdates
relative to the current date, so some users are born after their first order. The test was first configured with
`severity='warn'`, then fixed in exercise 3.

**Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the
sales per region?**

- **Age of the customers:** the birthdate is the measured value, so a wrong birthdate gives a wrong, even negative,
  age. The invalid birthdates must be replaced with `NULL` (and flagged), so that the users appear as "unknown age"
  instead of distorting the averages. The number of excluded users should be visible on the dashboard. The producer
  should also be asked to fix the generator.
- **Sales per region:** the birthdate is not used. Rejecting the users, or their orders, would remove real sales and
  underestimate the revenue. The records are kept and the issue is only monitored with a warning.

The strategy depends on the use of the data: the same anomaly is blocking for one consumer and irrelevant for another.
This is why the silver layer flags the issue (`birthdate_is_valid`) instead of deleting rows, and the gold layer decides.

**The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which
reference data would it require?**

A consistency test between the `state` and `zip_code` columns: it joins the users with a reference table of the valid
zip code ranges per state and returns the users whose zip code is outside the range of their state. It requires a
reference dataset mapping each state (including territories and military regions AA, AE, AP) to its zip code ranges,
or more precisely its 3-digit zip prefixes. The authoritative source is the USPS. It is implemented in exercise 4.

## Full pipeline and lineage

`dbt build` runs the seeds, models and tests in the order of the dependency graph: a test runs right after the model it
validates, and a model is skipped if an upstream test fails.

```bash
uv run dbt ls -q --resource-type source model --select +stg_orders
#> lab_medallion.silver.stg_orders
#> source:lab_medallion.bronze.orders
```

After exercise 3, `stg_users` depends on `stg_orders` to compute the first order date:

```text
source: bronze.orders ──→ stg_orders ──→ stg_users ←── source: bronze.users
                                             │
                          seed: states ──────┘ (tests)
```

`{{ source() }}` references an external dataset that dbt does not build, `{{ ref() }}` references another dbt resource.
Both are how dbt infers the graph. The documentation was generated with `dbt docs generate --static`.

## Export to Parquet

```bash
uv run duckdb lab_medallion.duckdb -c "
COPY (SELECT * FROM main_silver.stg_orders)
TO 's3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet' (FORMAT parquet);"
```

The Parquet file was compared with the view: same row count, an `EXCEPT` in both directions returns 0 rows, and
`DESCRIBE` shows the same types (`UUID`, `TIMESTAMP WITH TIME ZONE`, `INTEGER`, `VARCHAR`). Parquet keeps the types,
unlike CSV.

| | View `main_silver.stg_orders` | `silver/dbt/orders.parquet` |
| --- | --- | --- |
| What is stored | the SQL query | the transformed data |
| When the transformation runs | on every query | once, during the export |
| When `bronze/orders.csv` changes | up to date automatically | stale until the next export |
| Read cost | reads the whole CSV and recomputes | reads a compressed columnar file |

This is the difference between logical materialization (view) and physical materialization (Parquet).

## Exercises

### 1. Cleaned column

`username_normalized` is computed as `lower(trim(username))` in
[`stg_users.sql`](../../lab_medallion/models/silver/stg_users.sql). It is documented in `silver.yml` and tested with
`unique` and `not_null`: the normalized username is used to identify a user, so two usernames differing only by case or
whitespace would be a duplicate.

### 2. Order validation

| Rule | Test | Kind |
| --- | --- | --- |
| `quantity` is positive | [`assert_orders_quantity_positive.sql`](../../lab_medallion/tests/assert_orders_quantity_positive.sql) | singular, dbt has no built-in "greater than" generic test |
| `quantity` and `product` are present | `not_null` | generic |
| `product` belongs to the expected list | `accepted_values` | generic |
| every `user_id` exists in `stg_users` | `relationships` | generic |

Generic tests are preferred: they are declarative, documented next to the column, and reusable. A singular test is only
used when no generic test fits. `assert_orders_have_users` duplicates the `relationships` test and would be removed in
a real project; it is kept here because it is part of the lab.

### 3. Invalid birthdates

`stg_users` computes the first order of each user from `stg_orders`, then:

- `birthdate_is_valid` is true when the birthdate is not later than the first order, or when the user has no order
  (nothing contradicts the birthdate);
- `birthdate` is replaced with `NULL` when it is invalid.

Result: 5 users out of 50 have an invalid birthdate. The `severity='warn'` configuration was removed from
`assert_orders_after_user_birth`, which is a blocking test again and passes: a comparison with `NULL` returns no row.
`birthdate_is_valid` is tested with `not_null`.

The original value is not lost: it is still in the bronze layer, and the flag explains why the silver value is missing.

### 4. Invalid ZIP codes

- Seed [`seeds/states.csv`](../../lab_medallion/seeds/states.csv): one row per state, territory and military region
  (62 codes) with `zip_min` and `zip_max`, typed in [`seeds.yml`](../../lab_medallion/seeds/seeds.yml). The `code`
  column is tested with `unique` and `not_null`.
- Test [`assert_users_zip_matches_state.sql`](../../lab_medallion/tests/assert_users_zip_matches_state.sql): returns
  the users whose zip code is outside the range of their state. Configured with `severity='warn'`: 49 users out of 50
  are inconsistent, since the generator draws the zip code at random.
- The seed also fixes the existing `relationships` test of `stg_users.state` to `ref('states')`, which was silently
  disabled while the seed did not exist.

**Where should this reference data come from?** From an authoritative source, the USPS. For the lab, the ranges are
taken from the `states_postcode` table of the Faker `en_US` provider, completed with the military regions, which is a
simplification: one range per state is approximate, real zip codes are allocated by 3-digit prefixes.

**Should it be stored as a dbt seed?** Yes: it is a small, slowly changing table, which must be versioned with the code
and reviewed like the code. A large or frequently updated reference would instead be ingested as a bronze source.

**Should the validation happen in the silver layer?** Yes: the silver layer is where the data quality is enforced. The
test flags the anomaly without deleting the users, and the gold layer decides how to handle it for each use case.

**What happens when the reference data changes?** The CSV is updated in a commit and loaded with `dbt seed`; the next
`dbt build` re-evaluates the tests against the new reference. Git keeps the history of the reference used at each
point in time. Results computed before the change are not updated automatically if they were materialized as tables.

### 5. Materialization experiment

With `+materialized: table`, `dbt build --select silver` creates `BASE TABLE` relations instead of `VIEW` in the
`main_silver` schema, and the `lab_medallion.duckdb` file grows. The configuration was then restored to `view`.

| | View | Table |
| --- | --- | --- |
| Where is the data stored? | on S3, in the bronze CSV files; the view only stores SQL | copied into `lab_medallion.duckdb` |
| When is the transformation executed? | on every query | once, at `dbt run` / `dbt build` |
| What happens when the underlying CSV changes? | visible on the next query | invisible until the next `dbt build` |
| Which requires more storage? | almost none | the table |
| Which avoids re-reading the source for every query? | — | the table |

Views fit development and small, cheap models. Tables fit models that are expensive or queried often, at the cost of
storage and of a refresh that must be scheduled. Incremental models combine both for large append-only sources.
