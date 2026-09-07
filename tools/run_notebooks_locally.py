"""
Run the three Databricks notebooks locally, against the real CSVs, before spending a lab
attempt.

Parses the Databricks source format so the *actual* notebook files are what execute --
Python cells are exec'd, %sql cells go through spark.sql, %md cells are skipped. The only
thing rewritten is the three DBFS path constants (SOURCE_DIR, BRONZE_DIR, GOLD_DIR), which
are pointed at local directories; no notebook logic is touched.

Setup (once):

    python -m venv .venv-spark
    .venv-spark/Scripts/pip install "delta-spark==3.2.1"     # pulls pyspark 3.5.x

Windows also needs Hadoop's helper binaries, or Spark will not start:

    HADOOP_HOME=<dir containing bin/winutils.exe and bin/hadoop.dll>
    PATH=%HADOOP_HOME%\\bin;%PATH%
    PYSPARK_PYTHON=<full path to the venv python.exe>   # else "Python worker failed to connect back"

Run:

    DIVVY_DATA=/path/to/csvs  .venv-spark/Scripts/python tools/run_notebooks_locally.py

Environment variables:
    DIVVY_DATA        directory holding riders/payments/stations/trips CSVs (required)
    DIVVY_WORK        scratch directory for sample data, Delta output, warehouse
                      (default: ./.local-validation)
    TRIP_SAMPLE_ROWS  rows of trips.csv to use, 0 for all (default: 600000)
"""

import os
import re
import shutil
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, ".."))
NB_DIR = os.path.join(REPO, "notebooks")

SRC_DATA = os.environ.get("DIVVY_DATA")
WORK = os.environ.get("DIVVY_WORK", os.path.join(REPO, ".local-validation"))
TRIP_SAMPLE_ROWS = int(os.environ.get("TRIP_SAMPLE_ROWS", "600000"))

if not SRC_DATA or not os.path.isdir(SRC_DATA):
    sys.exit("Set DIVVY_DATA to the directory holding the four project CSVs.")


def p(*parts):
    return os.path.join(*parts).replace("\\", "/")


SAMPLE_DIR = p(WORK, "sample_data")
BRONZE_DIR = p(WORK, "delta", "bronze")
GOLD_DIR = p(WORK, "delta", "gold")
WAREHOUSE = p(WORK, "warehouse")

PATH_OVERRIDES = {
    "SOURCE_DIR": SAMPLE_DIR,
    "BRONZE_DIR": BRONZE_DIR,
    "GOLD_DIR": GOLD_DIR,
}

NOTEBOOKS = [
    "01_bronze_extract_load.py",
    "02_gold_dimensions.py",
    "03_gold_facts.py",
]


# --------------------------------------------------------------- sample data --
def build_sample():
    os.makedirs(SAMPLE_DIR, exist_ok=True)
    for name in ("riders.csv", "stations.csv", "payments.csv"):
        dst = p(SAMPLE_DIR, name)
        if not os.path.exists(dst):
            shutil.copyfile(p(SRC_DATA, name), dst)
            print("copied", name)

    dst = p(SAMPLE_DIR, "trips.csv")
    if os.path.exists(dst):
        return
    src = p(SRC_DATA, "trips.csv")
    if TRIP_SAMPLE_ROWS <= 0:
        shutil.copyfile(src, dst)
        print("copied trips.csv in full")
        return
    with open(src, "r", encoding="utf-8") as fin, \
         open(dst, "w", encoding="utf-8", newline="") as fout:
        for i, line in enumerate(fin):
            if i >= TRIP_SAMPLE_ROWS:
                break
            fout.write(line)
    print("sampled trips.csv ->", TRIP_SAMPLE_ROWS, "rows")


# ------------------------------------------------------------- dbutils stub --
class _FileInfo:
    def __init__(self, path, name):
        self.path = path
        self.name = name


class _FS:
    def ls(self, directory):
        d = directory.replace("dbfs:", "")
        return [_FileInfo(d.rstrip("/") + "/" + n, n) for n in sorted(os.listdir(d))]


class _DBUtils:
    fs = _FS()


# ------------------------------------------------------------ cell parsing --
def parse_cells(path):
    """Split a Databricks source file into ('py'|'sql'|'md', body) cells."""
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()

    cells = []
    for chunk in re.split(r"^# COMMAND -+\s*$", text, flags=re.MULTILINE):
        lines = chunk.splitlines()
        meaningful = [l for l in lines if l.strip()]
        if not meaningful:
            continue
        is_magic = all(
            l.lstrip().startswith("# MAGIC") or l.lstrip().startswith("# Databricks notebook")
            for l in meaningful
        )
        if is_magic:
            body = "\n".join(
                re.sub(r"^# MAGIC ?", "", l)
                for l in lines if l.lstrip().startswith("# MAGIC")
            ).strip()
            if body.startswith("%sql"):
                cells.append(("sql", body[len("%sql"):].strip()))
            else:
                cells.append(("md", body))
        else:
            cells.append(("py", chunk))
    return cells


def apply_overrides(code):
    for key, value in PATH_OVERRIDES.items():
        code = re.sub(r'^' + key + r'\s*=\s*"[^"]*"',
                      key + ' = "' + value + '"', code, flags=re.MULTILINE)
    return code


# ------------------------------------------------------------------ runner --
def main():
    build_sample()
    for d in (BRONZE_DIR, GOLD_DIR, WAREHOUSE):
        os.makedirs(d, exist_ok=True)

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("divvy-lakehouse-validation")
        .master("local[4]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.driver.memory", "6g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    env = {"spark": spark, "dbutils": _DBUtils(), "__name__": "__notebook__"}
    failures = []

    for nb in NOTEBOOKS:
        print("\n" + "=" * 78)
        print("NOTEBOOK:", nb)
        print("=" * 78)
        for i, (kind, body) in enumerate(parse_cells(os.path.join(NB_DIR, nb))):
            if kind == "md":
                continue
            label = nb + " cell " + str(i) + " [" + kind + "]"
            try:
                if kind == "py":
                    exec(compile(apply_overrides(body), label, "exec"), env)
                else:
                    print("\n--- %sql ---")
                    print(body.splitlines()[0][:100])
                    env["spark"].sql(body).show(5, truncate=False)
            except Exception:
                print("\n*** FAILED:", label)
                traceback.print_exc()
                failures.append(label)
                if kind == "py":
                    print("--- aborting notebook, later cells depend on this one ---")
                    break

    print("\n" + "=" * 78)
    if failures:
        print("FAILURES:", len(failures))
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("ALL CELLS PASSED")
    spark.stop()


if __name__ == "__main__":
    main()
