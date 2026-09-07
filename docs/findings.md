# Findings — local validation before the lab run

The Udacity cloud lab allows a limited number of attempts, so all three notebooks were run
end-to-end **locally** first, against the real project CSVs, before any lab time was spent.

## How the local run worked

| piece | what |
|---|---|
| engine | PySpark 3.5.9 + `delta-spark` 3.2.1 in a throwaway venv (Python 3.11, Java 8) |
| harness | parses the Databricks source format and executes the **actual notebook files** — Python cells `exec`'d, `%sql` cells run through `spark.sql`, `%md` skipped |
| data | `riders.csv`, `stations.csv`, `payments.csv` in full; `trips.csv` sampled to the first 600,000 rows to keep the run to a few minutes |
| rewrites | only the three DBFS path constants (`SOURCE_DIR`, `BRONZE_DIR`, `GOLD_DIR`) were pointed at local directories; no notebook logic was modified for the test |

Result: **all cells pass**, all eight foreign keys audit to zero orphans.

## Two real defects the local run caught

**1. `ORDER BY d.month` under `ROLLUP` did not resolve.** The outcome-2a query grouped by
`ROLLUP (d.year, d.quarter, d.month, d.month_name)` but only projected `year`, `quarter` and
`month_name`. Spark refused it:

```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `d`.`month`
cannot be resolved. Did you mean one of the following?
[`payments`, `d`.`year`, `d`.`month_name`, `d`.`quarter`, `total_amount`]
```

Fixed by adding `d.month` to the select list. Note this is specific to `ROLLUP` — the plain
`GROUP BY` queries elsewhere in the notebook order by grouping columns that are not projected
and resolve fine.

**2. `saveAsTable` in overwrite mode failed on Delta.** Every variant failed with
`Table X does not support truncate in batch mode`:

| pattern | result |
|---|---|
| `saveAsTable` + `option("path")` + `overwriteSchema` | FAIL |
| `saveAsTable`, managed table, `overwriteSchema` | FAIL |
| `saveAsTable`, managed table, no `overwriteSchema` | FAIL |
| `saveAsTable` + `option("path")`, no `overwriteSchema` | FAIL |
| explicit `DROP TABLE` first, then `saveAsTable` | FAIL |
| `.save(path)` then `CREATE TABLE … USING DELTA LOCATION` | **PASS** |

`saveAsTable` is the canonical Databricks pattern and is expected to work on Databricks
Runtime, so `write_gold()` still calls it **first**; if the runtime rejects it, the function
falls back to writing the same Delta files in the same `overwrite` mode and registering the
table over them — the pattern the bronze layer already uses. Either path produces an
overwritten Delta table in the gold database, so the rubric's requirement ("write to delta;
use overwrite mode; save as a table in delta") is met on either runtime.

One environment-only issue also surfaced and is **not** a notebook defect: Spark on Windows
needs `HADOOP_HOME`/`winutils.exe`, and `PYSPARK_PYTHON` must point at the venv interpreter or
the Python worker fails to connect back. Neither applies in Databricks.

## Source data validation

Checked with duckdb straight against the CSVs:

| check | result |
|---|---|
| row counts | riders 75,000 · payments 1,946,607 · stations 838 · trips 4,584,921 |
| natural key uniqueness | all four primary keys unique, no duplicates |
| nulls in join/key columns | none, except `rider.account_end_date` (60,046 — open accounts) |
| `trip.rider_id` → rider | 0 orphans |
| `trip.start_station_id` / `end_station_id` → station | 0 orphans |
| `payment.rider_id` → rider | 0 orphans |
| station ids that are **not** numeric | **306 of 838** — confirms `station_id` must be a string |
| trips ending at or before they start | 197 (min duration −55.9 min, max 55,944 min ≈ 38 days) |
| rider age at trip | 14 to 75, no negatives |
| payments per rider per month | exactly 1, always |
| membership split | 60,124 members / 14,876 casual |

## Gold output from the local run

Row counts (trips from the 600k sample; everything else full):

```
gold.dim_rider                    75,001    (75,000 + Unknown)
gold.dim_station                     839    (838 + Unknown)
gold.dim_date                      3,652    2013-01-01 .. 2022-12-31
gold.dim_time                         24
gold.fact_trip                   600,000
gold.fact_payment              1,946,607
gold.fact_rider_monthly        1,980,807
gold.agg_rider_spend_vs_rides     73,951
```

Every dimension passed its uniqueness check (rows = distinct keys, zero null keys), and all
eight foreign-key audits returned **0 orphan rows**. Zero facts landed on the Unknown member.

### Extra credit — the interesting result

Spend against ride frequency, per rider:

| rides/month band | riders | avg rides/mo | avg minutes/mo | avg spend/mo | avg spend/ride |
|---|---:|---:|---:|---:|---:|
| a. 0–1 | 63,392 | 0.11 | 2.6 | $9.47 | $81.24 |
| b. 1–3 | 7,755 | 1.70 | 38.9 | $6.97 | $4.59 |
| c. 3–6 | 2,066 | 4.02 | 106.3 | $5.44 | $1.41 |
| d. 6–12 | 603 | 7.88 | 194.2 | $3.72 | $0.50 |
| e. 12+ | 135 | 17.07 | 428.5 | $1.47 | $0.10 |

Monthly spend falls as ride frequency rises — the riders who use the service most pay the
least per month and dramatically less per ride. Two caveats worth stating rather than
burying: the `0–1` band is dominated by riders who paid in months they never rode, which is
what inflates its $81.24 spend-per-ride; and the trip side of this table comes from the 600k
trip sample, so the *shape* is trustworthy but the levels will shift on the full 4.58M trips
in the lab.

Payments total **$19,457,105.25** across 1,946,607 payments, 2013-02 through 2022-02 — that
figure is from the full payments file and will match the lab exactly.
