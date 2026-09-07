# Databricks notebook source
# MAGIC %md
# MAGIC # Divvy Bikeshare Lakehouse — 02 · Gold (Dimensions)
# MAGIC
# MAGIC **Author:** Tarie Nosworthy
# MAGIC
# MAGIC **Run order:** `01_bronze_extract_load` → **`02_gold_dimensions`** → `03_gold_facts`
# MAGIC
# MAGIC Builds the four conformed dimensions of the star schema from the bronze tables:
# MAGIC
# MAGIC | Dimension | Grain | Serves |
# MAGIC |---|---|---|
# MAGIC | `gold.dim_rider` | one row per rider | member vs casual, age at account start |
# MAGIC | `gold.dim_station` | one row per station | start / end station analysis |
# MAGIC | `gold.dim_date` | one row per calendar day | day of week, month, quarter, year |
# MAGIC | `gold.dim_time` | one row per hour of day | time of day |
# MAGIC
# MAGIC Each dimension **generates its own surrogate key**, retains its natural key for lineage
# MAGIC back to bronze, and holds **descriptive attributes only** — no additive measures. Each
# MAGIC is written to Delta in `overwrite` mode and saved as a table.
# MAGIC
# MAGIC > *Note on keys.* The Synapse version of this project keyed the dimensions on their
# MAGIC > natural keys, because a serverless SQL pool has no `IDENTITY` and CETAS cannot enforce
# MAGIC > a primary key, so a surrogate would have bought nothing. Spark has no such limitation:
# MAGIC > `row_number()` over an ordered window gives a dense, deterministic, rebuildable
# MAGIC > surrogate, so the dimensions here generate one and carry the natural key alongside it.

# COMMAND ----------

BRONZE_DB = "bronze"
GOLD_DIR = "dbfs:/delta/gold"
GOLD_DB = "gold"

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField

spark.conf.set("spark.sql.shuffle.partitions", 8)

# See notebook 01: Unity Catalog will not create tables over `dbfs:` locations, so pin
# the session to the Hive metastore where the bronze and gold Delta files live.
try:
    spark.sql("USE CATALOG hive_metastore")
    print("catalog: hive_metastore")
except Exception:
    print("catalog: workspace default (no Unity Catalog here)")

spark.sql("CREATE DATABASE IF NOT EXISTS " + GOLD_DB)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared helpers
# MAGIC
# MAGIC One writer, so every gold table is persisted the same way — **Delta format, overwrite
# MAGIC mode, saved as a table** at an explicit Delta location — and one age-banding expression,
# MAGIC so "30-39" means the same thing on the dimension and on the facts in notebook 03.

# COMMAND ----------

def write_gold(df, table_name):
    """Write `df` to Delta in overwrite mode and register it as gold.<table_name>.

    `saveAsTable` is the canonical Databricks pattern and is what runs here. Some
    Delta/Spark builds refuse an overwrite through the DataSource V2 catalog with
    "does not support truncate in batch mode"; on those, the fallback writes the same
    Delta files in the same overwrite mode and registers the table over them — the
    pattern the bronze layer already uses. Either way the result is identical: a Delta
    table, overwritten, registered in the gold database.
    """
    path = GOLD_DIR + "/" + table_name
    table = GOLD_DB + "." + table_name

    def delta_writer():
        return (df.write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true"))

    try:
        delta_writer().option("path", path).saveAsTable(table)
    except Exception as exc:
        print("  saveAsTable unavailable on this runtime (" + type(exc).__name__ +
              "); writing Delta files and registering the table instead")
        delta_writer().save(path)
        spark.sql("DROP TABLE IF EXISTS " + table)
        spark.sql("CREATE TABLE " + table + " USING DELTA LOCATION '" + path + "'")

    n = spark.table(table).count()
    print("wrote", table.ljust(22), format(n, ",").rjust(10), "rows ->", path)
    return n


def age_band(age_col):
    """Band an age column. Used by dim_rider and by both facts, so the labels agree."""
    return (
        F.when(age_col.isNull(), F.lit("unknown"))
         .when(age_col < 20, F.lit("under 20"))
         .when(age_col < 30, F.lit("20-29"))
         .when(age_col < 40, F.lit("30-39"))
         .when(age_col < 50, F.lit("40-49"))
         .when(age_col < 65, F.lit("50-64"))
         .otherwise(F.lit("65+"))
    )


def unknown_row(df, overrides):
    """Build the single 'Unknown' member for a dimension, matching `df` column for column.

    Derived columns come back from Spark marked non-nullable, so the row is built against a
    fully nullable copy of the schema; the union then widens cleanly.
    """
    nullable_schema = StructType([StructField(f.name, f.dataType, True) for f in df.schema.fields])
    values = tuple(overrides.get(f.name) for f in df.schema.fields)
    return spark.createDataFrame([values], nullable_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · `dim_rider`
# MAGIC
# MAGIC One row per rider.
# MAGIC
# MAGIC `age_at_account_start` is computed here rather than on a fact because it is a **fixed
# MAGIC property of the rider** — it never changes, no matter which payment you look at — and
# MAGIC business outcome 2b slices spend by exactly that. Age *at time of trip* is a different
# MAGIC animal: it moves with the trip date, so it belongs on `fact_trip` and is computed in
# MAGIC notebook 03.
# MAGIC
# MAGIC A surrogate key of `-1` ("Unknown") is added so a fact row referencing a rider that is
# MAGIC missing from the source still joins cleanly instead of vanishing from a report.

# COMMAND ----------

rider_src = spark.table(BRONZE_DB + ".rider")

dim_rider = (
    rider_src
    .withColumn("rider_key", F.row_number().over(Window.orderBy("rider_id")).cast("int"))
    .withColumnRenamed("first", "first_name")
    .withColumnRenamed("last", "last_name")
    .withColumn("rider_type", F.when(F.col("is_member"), F.lit("Member")).otherwise(F.lit("Casual")))
    .withColumn(
        "age_at_account_start",
        F.floor(F.months_between(F.col("account_start_date"), F.col("birthday")) / F.lit(12)).cast("int"),
    )
    .withColumn("age_band_at_account_start", age_band(F.col("age_at_account_start")))
    .withColumn("is_account_open", F.col("account_end_date").isNull())
    .select(
        "rider_key", "rider_id", "first_name", "last_name", "address", "birthday",
        "account_start_date", "account_end_date", "is_member", "rider_type",
        "age_at_account_start", "age_band_at_account_start", "is_account_open",
    )
)

unknown_rider = unknown_row(dim_rider, {
    "rider_key": -1,
    "rider_type": "Unknown",
    "age_band_at_account_start": "unknown",
})

write_gold(dim_rider.unionByName(unknown_rider), "dim_rider")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_rider WHERE rider_key > 0 ORDER BY rider_key LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · `dim_station`
# MAGIC
# MAGIC One row per station, deduplicated on the natural key. `dim_station` is a **role-playing
# MAGIC dimension**: `fact_trip` joins to it twice, once as the start station and once as the
# MAGIC end station.

# COMMAND ----------

station_src = (
    spark.table(BRONZE_DB + ".station")
    .withColumn("station_id", F.trim(F.col("station_id")))
    .filter(F.col("station_id").isNotNull())
    .dropDuplicates(["station_id"])
)

dim_station = (
    station_src
    .withColumn("station_key", F.row_number().over(Window.orderBy("station_id")).cast("int"))
    .withColumnRenamed("name", "station_name")
    .select("station_key", "station_id", "station_name", "latitude", "longitude")
)

unknown_station = unknown_row(dim_station, {
    "station_key": -1,
    "station_name": "Unknown",
})

write_gold(dim_station.unionByName(unknown_station), "dim_station")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_station WHERE station_key > 0 ORDER BY station_key LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `dim_date`
# MAGIC
# MAGIC One row per calendar day, keyed by a readable `yyyyMMdd` integer. The range is derived
# MAGIC from the data itself — the earliest and latest dates across trips, payments and rider
# MAGIC account dates — then padded out to whole years, so a partial first or last year still
# MAGIC rolls up correctly by quarter and year.

# COMMAND ----------

bounds = spark.sql("""
    SELECT MIN(d) AS min_date, MAX(d) AS max_date FROM (
        SELECT CAST(started_at AS DATE) AS d FROM bronze.trip
        UNION ALL SELECT CAST(ended_at AS DATE) FROM bronze.trip
        UNION ALL SELECT date               FROM bronze.payment
        UNION ALL SELECT account_start_date FROM bronze.rider
        UNION ALL SELECT account_end_date   FROM bronze.rider
    ) WHERE d IS NOT NULL
""").collect()[0]

start_date = bounds["min_date"].replace(month=1, day=1)
end_date = bounds["max_date"].replace(month=12, day=31)
print("data spans    ", bounds["min_date"], "to", bounds["max_date"])
print("dim_date spans", start_date, "to", end_date)

# COMMAND ----------

date_spine = spark.sql(
    "SELECT explode(sequence(to_date('" + str(start_date) + "'), "
    "to_date('" + str(end_date) + "'), interval 1 day)) AS full_date"
)

dim_date = (
    date_spine
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))          # 1 = Sunday
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
    .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    .withColumn("year_quarter", F.concat_ws("-Q", F.year("full_date"), F.quarter("full_date")))
    .withColumn("month_start_date", F.trunc("full_date", "month"))
    .select(
        "date_key", "full_date", "year", "quarter", "month", "month_name",
        "day_of_month", "day_of_week", "day_name", "week_of_year", "is_weekend",
        "year_month", "year_quarter", "month_start_date",
    )
)

write_gold(dim_date, "dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_date ORDER BY date_key LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · `dim_time`
# MAGIC
# MAGIC One row per hour of the day, keyed `0..23`. Hour grain is the right resolution for the
# MAGIC question actually being asked — "how long is a ride during the evening rush" — and
# MAGIC keeping time of day in its own dimension rather than as an hour column on the fact is
# MAGIC what lets that question be answered with a join instead of date arithmetic, while
# MAGIC `dim_date` stays at one clean row per day.

# COMMAND ----------

dim_time = (
    spark.range(0, 24)
    .withColumn("time_key", F.col("id").cast("int"))
    .withColumn("hour_24", F.col("id").cast("int"))
    .withColumn("hour_label", F.concat(F.lpad(F.col("id").cast("string"), 2, "0"), F.lit(":00")))
    .withColumn(
        "time_of_day",
        F.when(F.col("id") < 6, F.lit("Night"))
         .when(F.col("id") < 12, F.lit("Morning"))
         .when(F.col("id") < 17, F.lit("Afternoon"))
         .when(F.col("id") < 21, F.lit("Evening"))
         .otherwise(F.lit("Night")),
    )
    .withColumn("is_peak_commute", F.col("id").isin(7, 8, 9, 16, 17, 18))
    .select("time_key", "hour_24", "hour_label", "time_of_day", "is_peak_commute")
)

write_gold(dim_time, "dim_time")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dim_time ORDER BY time_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Verify the dimensions
# MAGIC
# MAGIC Surrogate keys must be unique and non-null, or the fact joins in notebook 03 fan out
# MAGIC and quietly inflate every measure in the schema.

# COMMAND ----------

checks = [
    ("dim_rider", "rider_key"),
    ("dim_station", "station_key"),
    ("dim_date", "date_key"),
    ("dim_time", "time_key"),
]

for table, key in checks:
    df = spark.table(GOLD_DB + "." + table)
    rows = df.count()
    distinct_keys = df.select(key).distinct().count()
    nulls = df.filter(F.col(key).isNull()).count()
    status = "OK" if (rows == distinct_keys and nulls == 0) else "FAIL"
    print(status.ljust(5), table.ljust(14),
          "rows=" + format(rows, ",").rjust(9),
          "distinct_keys=" + format(distinct_keys, ",").rjust(9),
          "null_keys=" + str(nulls))

# COMMAND ----------

spark.sql("SHOW TABLES IN " + GOLD_DB).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensions complete
# MAGIC
# MAGIC `gold.dim_rider`, `gold.dim_station`, `gold.dim_date` and `gold.dim_time` are written to
# MAGIC Delta and registered as tables. Continue with **`03_gold_facts`**.
