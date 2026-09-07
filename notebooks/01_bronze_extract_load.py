# Databricks notebook source
# MAGIC %md
# MAGIC # Divvy Bikeshare Lakehouse — 01 · Bronze (Extract + Load)
# MAGIC
# MAGIC **Run order:** `01_bronze_extract_load` → `02_gold_dimensions` → `03_gold_facts`
# MAGIC
# MAGIC | Step | What happens here |
# MAGIC |---|---|
# MAGIC | **Extract** | Read the four Divvy CSV files out of the Databricks file system and write them to **Delta files** under `/delta/bronze/...` |
# MAGIC | **Load** | Use `spark.sql` to create the **bronze database and tables** on top of those Delta files |
# MAGIC
# MAGIC The bronze store is a faithful, typed copy of the source system — no business logic,
# MAGIC no joins, no filtering, nothing dropped. All modelling happens downstream in the gold
# MAGIC notebooks, so bronze stays re-runnable and auditable against the raw CSVs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Configuration
# MAGIC
# MAGIC Upload the four project CSVs to DBFS first — `Data` → `Create Table` → `Upload File`,
# MAGIC or the DBFS file browser (enable it under `Admin Console` → `Workspace Settings` →
# MAGIC `Advanced` → `DBFS File Browser`). Uploads land in `dbfs:/FileStore/tables/` by default.
# MAGIC
# MAGIC `trips.csv` is ~440 MB, which is a slow browser upload. Spark reads gzip directly, so
# MAGIC uploading `trips.csv.gz` instead is roughly a quarter of the bytes and needs no code
# MAGIC change — the file resolver below accepts either.

# COMMAND ----------

SOURCE_DIR = "dbfs:/FileStore/tables"      # where the raw CSVs were uploaded
BRONZE_DIR = "dbfs:/delta/bronze"          # where the bronze Delta files are written
BRONZE_DB = "bronze"                       # bronze database (the bronze data store)

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType, BooleanType, DecimalType, DoubleType,
)

spark.conf.set("spark.sql.shuffle.partitions", 8)   # small data, single-node cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Locate the source files
# MAGIC
# MAGIC File names in the project archive have varied (`riders.csv` vs `rider.csv`), so each is
# MAGIC resolved by keyword rather than hard-coded. This also turns "I forgot to upload one"
# MAGIC into a readable error at the top of the notebook instead of a confusing empty table
# MAGIC three cells later.

# COMMAND ----------

def find_csv(directory, keyword):
    """Return the path of the CSV (or .csv.gz) in `directory` whose name contains `keyword`."""
    try:
        entries = dbutils.fs.ls(directory)
    except Exception:
        raise FileNotFoundError(
            "Source directory " + directory + " does not exist. "
            "Upload the project CSVs to DBFS and update SOURCE_DIR."
        )
    matches = [
        f.path for f in entries
        if keyword in f.name.lower()
        and (f.name.lower().endswith(".csv") or f.name.lower().endswith(".csv.gz"))
    ]
    if not matches:
        available = ", ".join(f.name for f in entries) or "(directory is empty)"
        raise FileNotFoundError(
            "No CSV matching '" + keyword + "' in " + directory + ". Found: " + available
        )
    return matches[0]


SOURCE_FILES = {
    "rider":   find_csv(SOURCE_DIR, "rider"),
    "payment": find_csv(SOURCE_DIR, "payment"),
    "station": find_csv(SOURCE_DIR, "station"),
    "trip":    find_csv(SOURCE_DIR, "trip"),
}

for name, path in SOURCE_FILES.items():
    print(name.ljust(8), "->", path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Source schemas
# MAGIC
# MAGIC The files are headerless, so column order comes from the relational ERD supplied with
# MAGIC the project. Declaring the schema explicitly also skips the inference pass, which on a
# MAGIC 440 MB `trips.csv` is a whole extra read of the file.
# MAGIC
# MAGIC One deliberate correction to the ERD: it types `Station.station_id` as `varchar` but
# MAGIC `Trip.start_station_id` / `Trip.end_station_id` as `int`. A key and its foreign key
# MAGIC cannot be different types, and the data settles the argument — `stations.csv` contains
# MAGIC both `525` and `KA1503000012`. **Station identifiers are strings throughout.**

# COMMAND ----------

RIDER_SCHEMA = StructType([
    StructField("rider_id",           IntegerType()),
    StructField("first",              StringType()),
    StructField("last",               StringType()),
    StructField("address",            StringType()),
    StructField("birthday",           DateType()),
    StructField("account_start_date", DateType()),
    StructField("account_end_date",   DateType()),
    StructField("is_member",          BooleanType()),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", IntegerType()),
    StructField("date",       DateType()),
    StructField("amount",     DecimalType(10, 2)),
    StructField("rider_id",   IntegerType()),
])

STATION_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("name",       StringType()),
    StructField("latitude",   DoubleType()),
    StructField("longitude",  DoubleType()),
])

TRIP_SCHEMA = StructType([
    StructField("trip_id",          StringType()),
    StructField("rideable_type",    StringType()),
    StructField("started_at",       TimestampType()),
    StructField("ended_at",         TimestampType()),
    StructField("start_station_id", StringType()),
    StructField("end_station_id",   StringType()),
    StructField("rider_id",         IntegerType()),
])

SCHEMAS = {
    "rider":   RIDER_SCHEMA,
    "payment": PAYMENT_SCHEMA,
    "station": STATION_SCHEMA,
    "trip":    TRIP_SCHEMA,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Reader
# MAGIC
# MAGIC The project files ship **without** a header row, but some copies in circulation have
# MAGIC one. Rather than guess, each file is read as raw strings, any row that turns out to be
# MAGIC a repeat of the column names is dropped, and only then are the declared types applied.
# MAGIC The ingest is correct either way, and a stray header can never masquerade as data.

# COMMAND ----------

def read_source_csv(path, schema):
    """Read a headerless (or header-bearing) CSV and return it typed to `schema`."""
    names = [f.name for f in schema.fields]
    string_schema = StructType([StructField(n, StringType()) for n in names])

    raw = (
        spark.read
        .option("header", "false")
        .option("mode", "PERMISSIVE")
        .schema(string_schema)
        .csv(path)
    )

    # Drop a header row if this copy of the file happens to have one.
    first_col = names[0]
    raw = raw.filter(F.lower(F.trim(F.col(first_col))) != F.lit(first_col.lower()))

    # Trim, turn empty strings into NULL, then cast to the declared types.
    return raw.select(*[
        F.when(F.length(F.trim(F.col(f.name))) == 0, None)
         .otherwise(F.trim(F.col(f.name)))
         .cast(f.dataType)
         .alias(f.name)
        for f in schema.fields
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · EXTRACT — CSV ➜ Delta files
# MAGIC
# MAGIC Each source file is written to its own Delta location. This is the extract step the
# MAGIC rubric asks for: data picked up from the Databricks file system and written out to
# MAGIC Delta file locations.

# COMMAND ----------

bronze_counts = {}

for name, path in SOURCE_FILES.items():
    df = read_source_csv(path, SCHEMAS[name])
    target = BRONZE_DIR + "/" + name

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(target))

    bronze_counts[name] = spark.read.format("delta").load(target).count()
    print("extracted", name.ljust(8), format(bronze_counts[name], ",").rjust(10),
          "rows ->", target)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · LOAD — Delta files ➜ bronze tables
# MAGIC
# MAGIC `spark.sql` creates the bronze database and registers a table over each Delta location
# MAGIC written above, so the raw layer is queryable as SQL. The tables are external: the Delta
# MAGIC files stay exactly where the extract step put them.

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS " + BRONZE_DB)

for name in SOURCE_FILES:
    spark.sql("DROP TABLE IF EXISTS " + BRONZE_DB + "." + name)
    spark.sql(
        "CREATE TABLE " + BRONZE_DB + "." + name + " "
        "USING DELTA "
        "LOCATION '" + BRONZE_DIR + "/" + name + "'"
    )
    print("created table", BRONZE_DB + "." + name)

spark.sql("SHOW TABLES IN " + BRONZE_DB).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Verify the bronze store
# MAGIC
# MAGIC Row counts, then a null check on every column the gold layer joins or keys on. A
# MAGIC problem caught here costs one cell; the same problem caught after the facts are built
# MAGIC costs a full rebuild.

# COMMAND ----------

for name in SOURCE_FILES:
    n = spark.table(BRONZE_DB + "." + name).count()
    print((BRONZE_DB + "." + name).ljust(16), format(n, ",").rjust(10), "rows")

# COMMAND ----------

spark.sql("""
    SELECT 'rider.rider_id'     AS column_checked, COUNT(*) AS null_rows FROM bronze.rider   WHERE rider_id   IS NULL
    UNION ALL SELECT 'rider.birthday',      COUNT(*) FROM bronze.rider   WHERE birthday   IS NULL
    UNION ALL SELECT 'rider.is_member',     COUNT(*) FROM bronze.rider   WHERE is_member  IS NULL
    UNION ALL SELECT 'payment.payment_id',  COUNT(*) FROM bronze.payment WHERE payment_id IS NULL
    UNION ALL SELECT 'payment.rider_id',    COUNT(*) FROM bronze.payment WHERE rider_id   IS NULL
    UNION ALL SELECT 'payment.date',        COUNT(*) FROM bronze.payment WHERE date       IS NULL
    UNION ALL SELECT 'payment.amount',      COUNT(*) FROM bronze.payment WHERE amount     IS NULL
    UNION ALL SELECT 'station.station_id',  COUNT(*) FROM bronze.station WHERE station_id IS NULL
    UNION ALL SELECT 'trip.trip_id',        COUNT(*) FROM bronze.trip    WHERE trip_id    IS NULL
    UNION ALL SELECT 'trip.started_at',     COUNT(*) FROM bronze.trip    WHERE started_at IS NULL
    UNION ALL SELECT 'trip.ended_at',       COUNT(*) FROM bronze.trip    WHERE ended_at   IS NULL
    UNION ALL SELECT 'trip.rider_id',       COUNT(*) FROM bronze.trip    WHERE rider_id   IS NULL
    UNION ALL SELECT 'trip.start_station_id', COUNT(*) FROM bronze.trip  WHERE start_station_id IS NULL
    UNION ALL SELECT 'trip.end_station_id',   COUNT(*) FROM bronze.trip  WHERE end_station_id   IS NULL
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.rider LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.payment LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.station LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.trip LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze complete
# MAGIC
# MAGIC `bronze.rider`, `bronze.payment`, `bronze.station` and `bronze.trip` are registered as
# MAGIC Delta tables over the files in `/delta/bronze/`. Continue with **`02_gold_dimensions`**.
