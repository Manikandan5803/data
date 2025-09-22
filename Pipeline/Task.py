# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from datetime import datetime

# Read config table
cfg = spark.table("demo.demo.config").collect()[0]
file_path, stage_table, raw_table = cfg["file_path"], cfg["stage_table"], cfg["raw_table"]

today = datetime.now().strftime("%Y-%m-%d")

# Stage layer (Bronze) as a VIEW to avoid storage conflicts
@dlt.view(
    name="stage_view",
    comment="Staging view with raw employee data"
)
def stage():
    return spark.read.option("header", "true").csv(file_path)

# Raw layer (Silver) as a TABLE with transformations
@dlt.table(
    name="demo.demo.raw_table_dlt",
    comment="Raw employee data with experience and audit columns (DLT managed)"
)
def raw():
    return (
        dlt.read("stage_view")  # ✅ Matches the stage view defined above
        .withColumn(
            "year_of_experience",
            (F.datediff(F.current_date(), F.col("join_date")) / 365).cast("int")
        )
        .withColumn("inserted_date", F.current_date())
        .withColumn("last_updated_date", F.current_date())
    )


# COMMAND ----------


