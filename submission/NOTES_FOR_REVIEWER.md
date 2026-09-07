# Notes for the reviewer

**Author:** Tarie Nosworthy

Three notebooks, run in order: `01_bronze_extract_load` → `02_gold_dimensions` →
`03_gold_facts`. All were executed on Azure Databricks against the full dataset and are
submitted with their cell outputs. The star schema is `star_schema.pdf`.

Everything below is a deliberate choice, flagged here so none of it reads as a mistake.

## Where the notebooks depart from the project instructions

**1. The lab's prescribed VM sizes were unavailable, so the cluster is a `Standard_D4ds_v4`.**

The environment page specifies `Standard_DS3_v2` or `Standard_DS4_v2` for a single-node
cluster. Both were stocked out in East US for the whole session:

```
CLOUD_PROVIDER_RESOURCE_STOCKOUT
SkuNotAvailable: The requested VM size ... 'Standard_DS3_v2' is currently not available
in location 'eastus'.
```

`DS3_v2` failed immediately; `DS4_v2` sat in "acquiring instances" for 30 minutes without
landing. Seven node types were tried before `Standard_D4ds_v4` got capacity. It is a 4-core
single-node cluster, so it stays well inside the 8-core regional limit the lab imposes — the
constraint that actually matters. Nothing about the pipeline depends on the SKU.

**2. Each notebook pins its Spark session to the Hive metastore.**

The lab workspace has Unity Catalog enabled, and Unity Catalog refuses to create a table
over a `dbfs:` location:

```
[UC_FILE_SCHEME_FOR_TABLE_CREATION_NOT_SUPPORTED] Creating table in Unity Catalog with
file scheme dbfs is not supported. SQLSTATE: 0AKUC
```

Both the bronze and gold stores live in DBFS, as the project intends, so with Unity Catalog
as the session's default catalog nothing past the bronze load can run. Each notebook
therefore begins with:

```python
try:
    spark.sql("USE CATALOG hive_metastore")
except Exception:
    pass   # workspace without Unity Catalog: the default catalog is already the metastore
```

It is guarded, so it is a no-op on a workspace without Unity Catalog. It is done in the
notebook rather than in the cluster's Spark configuration because a cluster-level setting
requires a restart, and with capacity that scarce, giving up a running node was not a
sensible trade.

**3. `write_gold()` falls back if `saveAsTable` is refused.**

The transform writes are `format("delta").mode("overwrite").saveAsTable(...)`, as the rubric
asks. On some Delta builds an overwrite through the DataSource V2 catalog is rejected with
`Table X does not support truncate in batch mode` — reproduced on every variant while
testing offline (managed and external, with and without `overwriteSchema`, even after an
explicit `DROP TABLE`). The helper therefore tries `saveAsTable` first and, if the runtime
refuses, writes the same Delta files in the same overwrite mode and registers the table over
them. Either path yields an overwritten Delta table in the gold database.

## Design decisions worth a sentence each

**Station identifiers are strings, not integers.** The supplied ERD types
`Station.station_id` as `varchar` but `Trip.start_station_id` / `end_station_id` as `int`. A
key and its foreign key cannot differ, and the data settles it: **306 of the 838 stations
have non-numeric ids** (`KA1503000012`). Typing them as `int` would silently null out 36% of
the dimension.

**Age at time of trip is a fact; age at account start is a dimension attribute.** The first
is point-in-time — the same rider is a different age on two different trips — so it lives on
`fact_trip`. The second is fixed per rider, so it lives on `dim_rider`, which is exactly what
business outcome 2b needs. Putting the first on the dimension would make it a
slowly-changing-dimension problem the project does not ask for.

**Nothing is dropped in the transform.** Dimension lookups are left joins onto an "Unknown"
member at key `-1`, and anomalies are counted rather than deleted — including the 197 trips
that end at or before they start. Silently dropping rows would stop the gold counts
reconciling with bronze, and the discrepancy would be invisible at query time.

**`dim_time` is a fourth dimension, at hour grain.** Business outcome 1 asks for time of
day. Keeping it in its own dimension lets that question be answered with a join instead of
date arithmetic, and keeps `dim_date` at one clean row per day.

## What the outputs show

`03_gold_facts` ends with an audit section and one query per business outcome. In the
submitted run, on the full dataset:

| table | rows |
|---|---:|
| `gold.fact_trip` | 4,584,921 |
| `gold.fact_payment` | 1,946,607 |
| `gold.fact_rider_monthly` *(extra credit)* | 2,049,370 |
| `gold.agg_rider_spend_vs_rides` *(extra credit)* | 74,116 |
| `gold.dim_rider` / `dim_station` / `dim_date` / `dim_time` | 75,001 / 839 / 3,652 / 24 |

All seven foreign-key audits return **zero orphan rows**, and no fact row landed on the
Unknown member. `fact_payment` totals **$19,457,105.25** across 2013-02-01 to 2022-02-01.

The extra credit answer: monthly spend falls from $9.59 to $5.94 as ride frequency rises,
while cost per ride collapses from $99.59 to $0.32 — the heaviest riders hold the same
subscription as the lightest and simply extract far more from it. The lowest-frequency band
is dominated by riders who paid in months they never rode, which is what inflates its
per-ride figure; that is a caveat on the number rather than a finding.

## Source

Full project including the design notes, the diagram generator and the offline validation
harness: https://github.com/tropibyte/Azure_Data_Lakehouse_Project
