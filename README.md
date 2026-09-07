# Building an Azure Data Lake for Bike Share Data Analytics

Divvy bikeshare lakehouse on **Azure Databricks + Delta Lake**: the four project CSVs are
extracted into a bronze Delta store, then transformed into a gold star schema that answers
the project's business outcomes.

Companion to [`Azure_Data_Warehouse_Project`](https://github.com/tropibyte/Azure_Data_Warehouse_Project),
which models the same Divvy data in an Azure Synapse serverless SQL pool. Same dataset, same
business questions, different engine — and one deliberate design difference, explained in
[§ Key strategy](docs/star_schema.md#key-strategy).

## Layout

```
notebooks/                     The graded notebooks, in run order
  01_bronze_extract_load.py    Extract CSV -> Delta files, then spark.sql creates bronze tables
  02_gold_dimensions.py        dim_rider, dim_station, dim_date, dim_time
  03_gold_facts.py             fact_trip, fact_payment, + extra credit, + audits and outcome queries
submission/                    The same three notebooks as executed .ipynb, with cell outputs
docs/
  star_schema.md               The design: source analysis, grain, keys, outcome coverage
  star_schema.pdf              Diagram for the submission zip
  star_schema.png              Same diagram, for the markdown
  findings.md                  What the local and lab runs proved, and the five defects they caught
  run_evidence/                Each notebook as executed on the lab cluster, HTML with outputs
tools/
  00_fetch_data_databricks.py  Helper notebook: pulls the dataset into DBFS from the driver
  render_star_schema.py        Regenerates the diagram (matplotlib)
  run_notebooks_locally.py     Runs the notebooks against local Spark before spending a lab attempt
```

The notebooks are in **Databricks source format**. Import them straight into a Databricks
workspace (`Workspace` → `Import` → `File`) and they arrive as notebooks with cells intact.

## Running it in the lab

1. **Start a cluster.** Single node, `Standard_DS3_v2`, Databricks runtime **≥ 10.0**. The lab
   caps you at 8 cores, so a single node is the safe choice.
2. **Upload the four CSVs** to DBFS — `Data` → `Create Table` → `Upload File`, or the DBFS
   file browser (enable it at `Admin Console` → `Workspace Settings` → `Advanced` →
   `DBFS File Browser`). They land in `dbfs:/FileStore/tables/`.
   `trips.csv` is ~440 MB; Spark reads gzip directly, so uploading `trips.csv.gz` instead is
   about a quarter of the bytes and needs no code change.
3. **Import the three notebooks** and attach them to the cluster.
4. **Run them in order:** `01_bronze_extract_load` → `02_gold_dimensions` → `03_gold_facts`.
   If `SOURCE_DIR` in notebook 01 does not match where you put the files, change that one
   constant; everything else is derived.
5. **Export for submission:** `File` → `Export` → `IPython Notebook` on each notebook, which
   captures the cell outputs alongside the code.

Zip the three exported `.ipynb` files together with `docs/star_schema.pdf` and submit.

## How this meets the rubric

| Rubric requirement | Where |
|---|---|
| Two fact tables sharing common dimensions | `fact_trip` and `fact_payment`, both keyed to `dim_date` and `dim_rider` |
| Trip fact has trip duration and rider age at time of trip | `fact_trip.duration_minutes`, `duration_seconds`, `rider_age_at_trip` |
| Payment fact has payment amount | `fact_payment.amount` |
| Trip dimensions: riders, stations, dates | `dim_rider`, `dim_station` (role-playing, start and end), `dim_date`, plus `dim_time` |
| Payment dimensions: dates, riders | `dim_date`, `dim_rider` |
| Python extracts CSV from Databricks storage and writes to Delta file locations | Notebook 01 § 4 — `read_source_csv()` then `.write.format("delta").save(...)` |
| `spark.sql` creates tables and loads from the extracted Delta files | Notebook 01 § 5 — `CREATE DATABASE` + `CREATE TABLE … USING DELTA LOCATION` |
| Fact scripts use the dimension keys and generate the correct facts | Notebook 03 § 1–3 |
| Dimension scripts match the diagram, generate keys, contain no facts | Notebook 02 § 1–4 |
| Transforms write to delta, overwrite mode, save as a table | `write_gold()` in notebooks 02 and 03 |
| Bronze data store and gold data store | `bronze` and `gold` databases, `/delta/bronze/` and `/delta/gold/` |
| **Extra credit** — spend per member vs rides and minutes per month | `fact_rider_monthly`, `agg_rider_spend_vs_rides`, notebook 03 § 3–4 |

## Design notes worth knowing before you read the code

* **Station ids are strings, not ints.** The classroom ERD types `Station.station_id` as
  `varchar` but the trip foreign keys as `int`. 306 of the 838 stations have non-numeric ids,
  so typing them as `int` would null out 36% of the dimension. See
  [`docs/star_schema.md § 1`](docs/star_schema.md).
* **Age at trip is a fact; age at account start is a dimension attribute.** One is
  point-in-time, the other is fixed per rider.
* **Nothing is silently dropped.** Dimension lookups are left joins onto an Unknown member at
  key `-1`, and notebook 03 audits every foreign key and counts every anomaly.
* **`write_gold()` has a fallback.** `saveAsTable` is tried first, and if the runtime refuses
  an overwrite through its V2 catalog, the same Delta files are written in the same overwrite
  mode and the table is registered over them. See [`docs/findings.md`](docs/findings.md) for
  why.

## Validated locally, then run for real

All three notebooks were run end-to-end **locally** against the real CSVs (PySpark 3.5.9 +
delta-spark 3.2.1) before spending a lab attempt, using
[`tools/run_notebooks_locally.py`](tools/run_notebooks_locally.py). That caught two genuine
defects. The **lab** run then caught three more that no local test could have — including a
Unity Catalog restriction that would have blocked everything past the bronze load. All five
are documented in [`docs/findings.md`](docs/findings.md).

Final run: Azure Databricks, DBR 14.3 LTS, single-node `Standard_D4ds_v4`, full dataset.

```
gold.fact_trip                 4,584,921      gold.dim_rider      75,001
gold.fact_payment              1,946,607      gold.dim_station       839
gold.fact_rider_monthly        2,049,370      gold.dim_date        3,652
gold.agg_rider_spend_vs_rides     74,116      gold.dim_time           24
```

All seven foreign-key audits returned **zero orphans**; no fact landed on the Unknown
member; `fact_payment` totals **$19,457,105.25**. Every figure predicted from the offline
duckdb pass matched exactly.
