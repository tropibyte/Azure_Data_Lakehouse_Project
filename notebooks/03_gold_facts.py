# Databricks notebook source
# MAGIC %md
# MAGIC # Divvy Bikeshare Lakehouse — 03 · Gold (Facts)
# MAGIC
# MAGIC **Run order:** `01_bronze_extract_load` → `02_gold_dimensions` → **`03_gold_facts`**
# MAGIC
# MAGIC Builds the fact tables at the centre of the star schema, using the surrogate keys
# MAGIC generated in notebook 02:
# MAGIC
# MAGIC | Fact | Grain | Measures |
# MAGIC |---|---|---|
# MAGIC | `gold.fact_trip` | one row per trip | `duration_seconds`, `duration_minutes`, `rider_age_at_trip` |
# MAGIC | `gold.fact_payment` | one row per payment | `amount`, `rider_age_at_payment` |
# MAGIC | `gold.fact_rider_monthly` | one row per rider per active month | rides, ride minutes, amount paid *(extra credit)* |
# MAGIC | `gold.agg_rider_spend_vs_rides` | one row per rider | spend vs ride frequency *(extra credit)* |
# MAGIC
# MAGIC `dim_rider` and `dim_date` are **conformed** — both transaction facts key to them, which
# MAGIC is what allows spend and ride behaviour to be compared for the same rider on the same
# MAGIC calendar.

# COMMAND ----------

BRONZE_DB = "bronze"
GOLD_DIR = "dbfs:/delta/gold"
GOLD_DB = "gold"

from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", 8)


def write_gold(df, table_name):
    """Write `df` to Delta in overwrite mode and register it as gold.<table_name>.

    Same writer as notebook 02: `saveAsTable` first, falling back to writing the Delta
    files and registering the table over them on runtimes whose V2 catalog refuses an
    overwrite. Either path produces a Delta table, overwritten, in the gold database.
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
    print("wrote", table.ljust(28), format(n, ",").rjust(10), "rows ->", path)
    return n


def age_band(age_col):
    """Same banding as dim_rider, so '30-39' means the same thing everywhere."""
    return (
        F.when(age_col.isNull(), F.lit("unknown"))
         .when(age_col < 20, F.lit("under 20"))
         .when(age_col < 30, F.lit("20-29"))
         .when(age_col < 40, F.lit("30-39"))
         .when(age_col < 50, F.lit("40-49"))
         .when(age_col < 65, F.lit("50-64"))
         .otherwise(F.lit("65+"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · `fact_trip`
# MAGIC
# MAGIC Grain: **one row per trip**.
# MAGIC
# MAGIC * Foreign keys to `dim_rider`, `dim_station` (twice — start and end), `dim_date` (twice)
# MAGIC   and `dim_time` (twice).
# MAGIC * `trip_id` and `rideable_type` ride along as degenerate dimensions.
# MAGIC * `duration_minutes` is the additive measure business outcome 1 is built on.
# MAGIC * `rider_age_at_trip` is computed **against the trip date**, not the account start date,
# MAGIC   so a rider who has been on the platform for years is counted at the age they actually
# MAGIC   were on the day of the ride. That is a point-in-time value, which is why it is a fact
# MAGIC   and not a rider attribute — storing it on `dim_rider` would be a slowly-changing
# MAGIC   dimension problem this project does not need.
# MAGIC * `is_member` / `rider_type` are carried onto the fact so outcome 1d needs no join.
# MAGIC
# MAGIC Lookups are **left joins**, so no trip is ever silently dropped; anything that fails to
# MAGIC match lands on the `-1` Unknown member and is counted in the audit section below.

# COMMAND ----------

trip_src = (
    spark.table(BRONZE_DB + ".trip")
    .withColumn("start_station_id", F.trim(F.col("start_station_id")))
    .withColumn("end_station_id", F.trim(F.col("end_station_id")))
)

rider_lkp = (
    spark.table(GOLD_DB + ".dim_rider")
    .select("rider_key", "rider_id", "birthday", "is_member", "rider_type")
)

start_station_lkp = (
    spark.table(GOLD_DB + ".dim_station")
    .select(F.col("station_key").alias("start_station_key"),
            F.col("station_id").alias("start_station_id"))
)

end_station_lkp = (
    spark.table(GOLD_DB + ".dim_station")
    .select(F.col("station_key").alias("end_station_key"),
            F.col("station_id").alias("end_station_id"))
)

# COMMAND ----------

fact_trip = (
    trip_src
    .join(rider_lkp, on="rider_id", how="left")
    .join(start_station_lkp, on="start_station_id", how="left")
    .join(end_station_lkp, on="end_station_id", how="left")

    # --- dimension keys -------------------------------------------------
    .withColumn("rider_key", F.coalesce(F.col("rider_key"), F.lit(-1)))
    .withColumn("start_station_key", F.coalesce(F.col("start_station_key"), F.lit(-1)))
    .withColumn("end_station_key", F.coalesce(F.col("end_station_key"), F.lit(-1)))
    .withColumn("start_date_key", F.date_format("started_at", "yyyyMMdd").cast("int"))
    .withColumn("end_date_key", F.date_format("ended_at", "yyyyMMdd").cast("int"))
    .withColumn("start_time_key", F.hour("started_at").cast("int"))
    .withColumn("end_time_key", F.hour("ended_at").cast("int"))

    # --- measures -------------------------------------------------------
    .withColumn(
        "duration_seconds",
        (F.col("ended_at").cast("long") - F.col("started_at").cast("long")).cast("int"),
    )
    .withColumn(
        "duration_minutes",
        F.round(F.col("duration_seconds") / F.lit(60.0), 2).cast("decimal(10,2)"),
    )
    .withColumn(
        "rider_age_at_trip",
        F.floor(F.months_between(F.to_date("started_at"), F.col("birthday")) / F.lit(12)).cast("int"),
    )
    .withColumn("rider_age_band_at_trip", age_band(F.col("rider_age_at_trip")))

    .select(
        "trip_id",
        "rider_key",
        "start_station_key",
        "end_station_key",
        "start_date_key",
        "end_date_key",
        "start_time_key",
        "end_time_key",
        "rideable_type",
        "started_at",
        "ended_at",
        "duration_seconds",
        "duration_minutes",
        "rider_age_at_trip",
        "rider_age_band_at_trip",
        "is_member",
        "rider_type",
    )
)

write_gold(fact_trip, "fact_trip")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.fact_trip LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · `fact_payment`
# MAGIC
# MAGIC Grain: **one row per payment**. `amount` is the additive measure. The rider's age at
# MAGIC *account start* lives on `dim_rider` because it is fixed per rider; age at *payment* is
# MAGIC point-in-time and so sits here on the fact, exactly as `rider_age_at_trip` does.

# COMMAND ----------

payment_src = spark.table(BRONZE_DB + ".payment")

payment_rider_lkp = (
    spark.table(GOLD_DB + ".dim_rider").select("rider_key", "rider_id", "birthday")
)

fact_payment = (
    payment_src
    .join(payment_rider_lkp, on="rider_id", how="left")
    .withColumn("rider_key", F.coalesce(F.col("rider_key"), F.lit(-1)))
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn(
        "rider_age_at_payment",
        F.floor(F.months_between(F.col("date"), F.col("birthday")) / F.lit(12)).cast("int"),
    )
    .withColumnRenamed("date", "payment_date")
    .select("payment_id", "rider_key", "date_key", "payment_date", "amount",
            "rider_age_at_payment")
)

write_gold(fact_payment, "fact_payment")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.fact_payment LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `fact_rider_monthly` — extra credit
# MAGIC
# MAGIC Grain: **one row per rider per calendar month in which the rider was active.**
# MAGIC
# MAGIC The third business outcome compares money spent against ride *behaviour* — rides per
# MAGIC month and minutes per month. Those live in two different facts at two different grains,
# MAGIC so they are conformed here onto a common monthly grain rather than joined at query
# MAGIC time. Joining `fact_trip` to `fact_payment` directly would fan out — every trip against
# MAGIC every payment for that rider — and silently multiply both measures. Aggregating each
# MAGIC side first and then outer-joining keeps the arithmetic honest.
# MAGIC
# MAGIC The outer join is deliberate: a rider can pay in a month with no rides, and ride in a
# MAGIC month with no payment. Either would be dropped by an inner join.

# COMMAND ----------

date_lkp = spark.table(GOLD_DB + ".dim_date").select("date_key", "month_start_date")

trip_month = (
    spark.table(GOLD_DB + ".fact_trip")
    .join(date_lkp, F.col("start_date_key") == F.col("date_key"), "left")
    .groupBy("rider_key", "month_start_date")
    .agg(
        F.count(F.lit(1)).cast("int").alias("rides_in_month"),
        F.sum("duration_minutes").cast("decimal(12,2)").alias("ride_minutes_in_month"),
    )
)

payment_month = (
    spark.table(GOLD_DB + ".fact_payment")
    .join(date_lkp, on="date_key", how="left")
    .groupBy("rider_key", "month_start_date")
    .agg(
        F.count(F.lit(1)).cast("int").alias("payments_in_month"),
        F.sum("amount").cast("decimal(12,2)").alias("amount_paid_in_month"),
    )
)

fact_rider_monthly = (
    trip_month
    .join(payment_month, on=["rider_key", "month_start_date"], how="full_outer")
    .withColumn("rides_in_month", F.coalesce(F.col("rides_in_month"), F.lit(0)))
    .withColumn("ride_minutes_in_month",
                F.coalesce(F.col("ride_minutes_in_month"), F.lit(0).cast("decimal(12,2)")))
    .withColumn("payments_in_month", F.coalesce(F.col("payments_in_month"), F.lit(0)))
    .withColumn("amount_paid_in_month",
                F.coalesce(F.col("amount_paid_in_month"), F.lit(0).cast("decimal(12,2)")))
    .withColumn("month_date_key", F.date_format("month_start_date", "yyyyMMdd").cast("int"))
    .withColumn("year_month", F.date_format("month_start_date", "yyyy-MM"))
    .withColumn("year", F.year("month_start_date"))
    .withColumn("quarter", F.quarter("month_start_date"))
    .withColumn("month", F.month("month_start_date"))
    .select("rider_key", "month_date_key", "month_start_date", "year_month",
            "year", "quarter", "month", "rides_in_month", "ride_minutes_in_month",
            "payments_in_month", "amount_paid_in_month")
)

write_gold(fact_rider_monthly, "fact_rider_monthly")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.fact_rider_monthly ORDER BY rider_key, month_date_key LIMIT 12

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · `agg_rider_spend_vs_rides` — extra credit
# MAGIC
# MAGIC Grain: **one row per rider.** Rolls the monthly fact up to lifetime totals and derives
# MAGIC the ratios the business outcome actually asks for.
# MAGIC
# MAGIC `months_observed` — first to last active month inclusive — is the denominator rather
# MAGIC than a raw count of rows, so a rider who took a three-month break is not flattered by
# MAGIC having their spend divided only by the months they showed up. `avg_spend_per_month` is
# MAGIC then tenure-neutral: a rider who paid for twelve months and one who paid for two are
# MAGIC compared on the same footing.

# COMMAND ----------

monthly = spark.table(GOLD_DB + ".fact_rider_monthly")

rider_attrs = (
    spark.table(GOLD_DB + ".dim_rider")
    .select("rider_key", "rider_type", "is_member",
            "age_at_account_start", "age_band_at_account_start")
)

agg_rider_spend_vs_rides = (
    monthly
    .groupBy("rider_key")
    .agg(
        F.min("month_start_date").alias("first_active_month"),
        F.max("month_start_date").alias("last_active_month"),
        F.sum(F.when(F.col("rides_in_month") > 0, 1).otherwise(0)).cast("int").alias("months_active"),
        F.sum("rides_in_month").cast("int").alias("total_rides"),
        F.sum("ride_minutes_in_month").cast("decimal(12,2)").alias("total_ride_minutes"),
        F.sum("amount_paid_in_month").cast("decimal(12,2)").alias("total_paid"),
    )
    .withColumn(
        "months_observed",
        (F.floor(F.months_between(F.col("last_active_month"), F.col("first_active_month"))) + F.lit(1)).cast("int"),
    )
    .withColumn(
        "avg_rides_per_month",
        F.round(F.col("total_rides") / F.col("months_observed"), 2).cast("decimal(10,2)"),
    )
    .withColumn(
        "avg_spend_per_month",
        F.round(F.col("total_paid") / F.col("months_observed"), 2).cast("decimal(12,2)"),
    )
    .withColumn(
        "spend_per_ride",
        F.when(F.col("total_rides") > 0,
               F.round(F.col("total_paid") / F.col("total_rides"), 2)).cast("decimal(12,2)"),
    )
    .withColumn(
        "rides_per_month_band",
        F.when(F.col("avg_rides_per_month") < 1, F.lit("a. 0-1"))
         .when(F.col("avg_rides_per_month") < 3, F.lit("b. 1-3"))
         .when(F.col("avg_rides_per_month") < 6, F.lit("c. 3-6"))
         .when(F.col("avg_rides_per_month") < 12, F.lit("d. 6-12"))
         .otherwise(F.lit("e. 12+")),
    )
    .join(rider_attrs, on="rider_key", how="left")
    .select("rider_key", "rider_type", "is_member", "age_at_account_start",
            "age_band_at_account_start", "first_active_month", "last_active_month",
            "months_observed", "months_active", "total_rides", "total_ride_minutes",
            "total_paid", "avg_rides_per_month", "avg_spend_per_month",
            "spend_per_ride", "rides_per_month_band")
)

write_gold(agg_rider_spend_vs_rides, "agg_rider_spend_vs_rides")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.agg_rider_spend_vs_rides ORDER BY rider_key LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Audit the star schema
# MAGIC
# MAGIC Referential integrity first — every foreign key on a fact must resolve to a real row in
# MAGIC its dimension — then a data-quality read on the trip measures.

# COMMAND ----------

spark.sql("""
    SELECT 'fact_trip.rider_key -> dim_rider' AS relationship, COUNT(*) AS orphan_rows
      FROM gold.fact_trip f LEFT ANTI JOIN gold.dim_rider d ON f.rider_key = d.rider_key
    UNION ALL
    SELECT 'fact_trip.start_station_key -> dim_station', COUNT(*)
      FROM gold.fact_trip f LEFT ANTI JOIN gold.dim_station d ON f.start_station_key = d.station_key
    UNION ALL
    SELECT 'fact_trip.end_station_key -> dim_station', COUNT(*)
      FROM gold.fact_trip f LEFT ANTI JOIN gold.dim_station d ON f.end_station_key = d.station_key
    UNION ALL
    SELECT 'fact_trip.start_date_key -> dim_date', COUNT(*)
      FROM gold.fact_trip f LEFT ANTI JOIN gold.dim_date d ON f.start_date_key = d.date_key
    UNION ALL
    SELECT 'fact_trip.end_date_key -> dim_date', COUNT(*)
      FROM gold.fact_trip f LEFT ANTI JOIN gold.dim_date d ON f.end_date_key = d.date_key
    UNION ALL
    SELECT 'fact_trip.start_time_key -> dim_time', COUNT(*)
      FROM gold.fact_trip f LEFT ANTI JOIN gold.dim_time d ON f.start_time_key = d.time_key
    UNION ALL
    SELECT 'fact_payment.rider_key -> dim_rider', COUNT(*)
      FROM gold.fact_payment f LEFT ANTI JOIN gold.dim_rider d ON f.rider_key = d.rider_key
    UNION ALL
    SELECT 'fact_payment.date_key -> dim_date', COUNT(*)
      FROM gold.fact_payment f LEFT ANTI JOIN gold.dim_date d ON f.date_key = d.date_key
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC How many facts landed on the `-1` Unknown member, and how do the trip measures look?
# MAGIC Non-positive durations are **reported, not deleted** — the gold layer stays a complete
# MAGIC account of what the source system said, and an analyst can filter them at query time.

# COMMAND ----------

spark.sql("""
    SELECT
        COUNT(*)                                                AS trips,
        SUM(CASE WHEN rider_key         = -1 THEN 1 ELSE 0 END) AS unknown_rider,
        SUM(CASE WHEN start_station_key = -1 THEN 1 ELSE 0 END) AS unknown_start_station,
        SUM(CASE WHEN end_station_key   = -1 THEN 1 ELSE 0 END) AS unknown_end_station,
        SUM(CASE WHEN duration_minutes <= 0 THEN 1 ELSE 0 END)  AS non_positive_duration,
        ROUND(MIN(duration_minutes), 2)                         AS min_minutes,
        ROUND(AVG(duration_minutes), 2)                         AS avg_minutes,
        ROUND(MAX(duration_minutes), 2)                         AS max_minutes,
        MIN(rider_age_at_trip)                                  AS min_age,
        MAX(rider_age_at_trip)                                  AS max_age
    FROM gold.fact_trip
""").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW TABLES IN " + GOLD_DB).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · The business outcomes, answered
# MAGIC
# MAGIC Each query below is one of the questions from the project brief, run against the star
# MAGIC schema exactly as an analyst would run it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outcome 1a — time spent per ride, by day of week and time of day

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.day_name,
# MAGIC     t.time_of_day,
# MAGIC     COUNT(*)                          AS rides,
# MAGIC     ROUND(AVG(f.duration_minutes), 2) AS avg_minutes
# MAGIC FROM gold.fact_trip f
# MAGIC JOIN gold.dim_date d ON f.start_date_key = d.date_key
# MAGIC JOIN gold.dim_time t ON f.start_time_key = t.time_key
# MAGIC GROUP BY d.day_name, d.day_of_week, t.time_of_day
# MAGIC ORDER BY d.day_of_week, avg_minutes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outcome 1b — time spent per ride, by starting station (top 15 by volume)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.station_name                    AS start_station,
# MAGIC     COUNT(*)                          AS rides,
# MAGIC     ROUND(AVG(f.duration_minutes), 2) AS avg_minutes
# MAGIC FROM gold.fact_trip f
# MAGIC JOIN gold.dim_station s ON f.start_station_key = s.station_key
# MAGIC GROUP BY s.station_name
# MAGIC ORDER BY rides DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outcome 1c / 1d — time spent per ride, by rider age at time of trip and membership

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     f.rider_age_band_at_trip          AS age_band_at_trip,
# MAGIC     f.rider_type,
# MAGIC     COUNT(*)                          AS rides,
# MAGIC     ROUND(AVG(f.duration_minutes), 2) AS avg_minutes
# MAGIC FROM gold.fact_trip f
# MAGIC GROUP BY f.rider_age_band_at_trip, f.rider_type
# MAGIC ORDER BY age_band_at_trip, f.rider_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outcome 2a — money spent per month, quarter and year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.quarter,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     COUNT(*)                AS payments,
# MAGIC     ROUND(SUM(p.amount), 2) AS total_amount
# MAGIC FROM gold.fact_payment p
# MAGIC JOIN gold.dim_date d ON p.date_key = d.date_key
# MAGIC GROUP BY ROLLUP (d.year, d.quarter, d.month, d.month_name)
# MAGIC ORDER BY d.year, d.quarter, d.month

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outcome 2b — money spent per rider, by age at account start

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     r.age_band_at_account_start,
# MAGIC     r.rider_type,
# MAGIC     COUNT(DISTINCT r.rider_key)                          AS riders,
# MAGIC     ROUND(SUM(p.amount), 2)                              AS total_amount,
# MAGIC     ROUND(SUM(p.amount) / COUNT(DISTINCT r.rider_key), 2) AS amount_per_rider
# MAGIC FROM gold.fact_payment p
# MAGIC JOIN gold.dim_rider r ON p.rider_key = r.rider_key
# MAGIC GROUP BY r.age_band_at_account_start, r.rider_type
# MAGIC ORDER BY r.age_band_at_account_start, r.rider_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outcome 3 (extra credit) — money spent per rider vs rides and minutes per month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     rides_per_month_band,
# MAGIC     COUNT(*)                                             AS riders,
# MAGIC     ROUND(AVG(avg_rides_per_month), 2)                   AS avg_rides_per_month,
# MAGIC     ROUND(AVG(total_ride_minutes / months_observed), 1)  AS avg_minutes_per_month,
# MAGIC     ROUND(AVG(avg_spend_per_month), 2)                   AS avg_spend_per_month,
# MAGIC     ROUND(AVG(spend_per_ride), 2)                        AS avg_spend_per_ride,
# MAGIC     ROUND(AVG(total_paid), 2)                            AS avg_lifetime_spend
# MAGIC FROM gold.agg_rider_spend_vs_rides
# MAGIC WHERE rider_key <> -1
# MAGIC GROUP BY rides_per_month_band
# MAGIC ORDER BY rides_per_month_band

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold complete
# MAGIC
# MAGIC The gold data store holds four dimensions and four facts, every one a Delta table:
# MAGIC
# MAGIC ```
# MAGIC gold.dim_rider     gold.fact_trip
# MAGIC gold.dim_station   gold.fact_payment
# MAGIC gold.dim_date      gold.fact_rider_monthly         (extra credit)
# MAGIC gold.dim_time      gold.agg_rider_spend_vs_rides   (extra credit)
# MAGIC ```
