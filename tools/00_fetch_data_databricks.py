# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Fetch the project data into DBFS
# MAGIC
# MAGIC **Helper notebook — not part of the graded submission.**
# MAGIC
# MAGIC Downloads the course dataset archive straight to the cluster driver and unpacks the
# MAGIC four CSVs into `dbfs:/FileStore/tables/`, which is where `01_bronze_extract_load`
# MAGIC expects them.
# MAGIC
# MAGIC This exists because `trips.csv` is ~440 MB uncompressed. Pushing that through the
# MAGIC browser uploader is slow and easy to get wrong; pulling the 128 MB archive from the
# MAGIC driver takes about a minute and makes the whole pipeline reproducible from an empty
# MAGIC workspace. Uploading the files by hand works exactly as well — this is a convenience,
# MAGIC not a requirement.
# MAGIC
# MAGIC Safe to re-run: it skips the download if the CSVs are already in place.

# COMMAND ----------

DATA_URL = (
    "https://video.udacity-data.com/topher/2022/March/"
    "62420bb1_azure-data-lakehouse-projectdatafiles/"
    "azure-data-lakehouse-projectdatafiles.zip"
)
TARGET_DIR = "dbfs:/FileStore/tables"
WORK_DIR = "/local_disk0/tmp/divvy"

EXPECTED = ["riders.csv", "payments.csv", "stations.csv", "trips.csv"]

import os
import shutil
import urllib.request
import zipfile

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Skip the work if the data is already there

# COMMAND ----------

def existing_csvs():
    try:
        return {f.name.rstrip("/").lower() for f in dbutils.fs.ls(TARGET_DIR)}
    except Exception:
        return set()


present = existing_csvs()
missing = [name for name in EXPECTED if name.lower() not in present]

print("already in", TARGET_DIR + ":", sorted(present) or "(nothing)")
print("missing:", missing or "(none — nothing to do)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Download and unpack

# COMMAND ----------

if missing:
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    os.makedirs(WORK_DIR, exist_ok=True)
    archive = os.path.join(WORK_DIR, "divvy.zip")

    # The CDN rejects the default urllib User-Agent with HTTP 403 — it allows browsers
    # but blocks "Python-urllib/x.y" outright. Send a browser UA and it serves the file.
    print("downloading", DATA_URL)
    request = urllib.request.Request(
        DATA_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
        },
    )
    with urllib.request.urlopen(request) as response, open(archive, "wb") as out:
        shutil.copyfileobj(response, out)
    print("downloaded", "{:,}".format(os.path.getsize(archive)), "bytes")

    with zipfile.ZipFile(archive) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        print("archive contains:", members)
        zf.extractall(WORK_DIR)

    # The archive may nest the CSVs in a folder; find them wherever they landed.
    found = {}
    for root, _dirs, files in os.walk(WORK_DIR):
        for fname in files:
            if fname.lower().endswith(".csv"):
                found[fname.lower()] = os.path.join(root, fname)

    for name in EXPECTED:
        src = found.get(name.lower())
        if src is None:
            raise FileNotFoundError(
                name + " not found in the archive. Present: " + ", ".join(sorted(found))
            )
        size = os.path.getsize(src)
        dbutils.fs.cp("file:" + src.replace("\\", "/"), TARGET_DIR + "/" + name)
        print("copied", name.ljust(14), "{:>12,}".format(size), "bytes -> " + TARGET_DIR)

    shutil.rmtree(WORK_DIR, ignore_errors=True)
else:
    print("nothing to download")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Verify

# COMMAND ----------

for f in sorted(dbutils.fs.ls(TARGET_DIR), key=lambda x: x.name):
    if f.name.lower().endswith(".csv"):
        print(f.name.ljust(16), "{:>12,}".format(f.size), "bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC Row counts, as a cheap sanity check before the bronze notebook runs.
# MAGIC Expect 75,000 riders · 1,946,607 payments · 838 stations · 4,584,921 trips.

# COMMAND ----------

for name in EXPECTED:
    n = spark.read.option("header", "false").csv(TARGET_DIR + "/" + name).count()
    print(name.ljust(16), "{:>12,}".format(n), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Data is in place. Continue with **`01_bronze_extract_load`**.
