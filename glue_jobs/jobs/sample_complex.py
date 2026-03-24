import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Shared SAS-compatibility utilities
sys.path.insert(0, "s3://glue-scripts-bucket/common/")
from transform_utils import sas_date_to_spark, first_last_flags  # noqa: E402

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Source: sample_complex.sas
# Converted: 2026-03-10T12:35:43.952407+00:00
# Tier: RED
# ---------------------------------------------------------------------------


# %INCLUDE '/macros/date_utils.sas' -- import equivalent module


# %INCLUDE '/macros/validation.sas' -- import equivalent module


# MACRO %process_monthly -- macro logic must be inlined or converted to Python function


# DATA work.monthly_extract; SET raw.transactions;
df_raw_transactions = spark.table("glue_catalog.raw.transactions")
df_monthly_extract = df_raw_transactions
df_monthly_extract = df_monthly_extract.filter("txn_date BETWEEN &start_dt AND &end_dt")
df_monthly_extract = df_monthly_extract.select("txn_id", "customer_id", "txn_date", "amount", "category", "store_id")

df_monthly_extract.write.mode("overwrite").saveAsTable("glue_catalog.work.monthly_extract")


# CREATE TABLE work.customer_txns
df_customer_txns = spark.sql("""
SELECT a.*, b.segment, b.region, b
        FROM work a
        LEFT JOIN raw b
            ON a = b.customer_id
""")
df_customer_txns.write.mode("overwrite").saveAsTable("glue_catalog.work.customer_txns")



# DATA work.enriched_txns; SET work.customer_txns;
df_customer_txns = spark.table("glue_catalog.work.customer_txns")
df_enriched_txns = df_customer_txns
df_enriched_txns = df_enriched_txns.withColumn("amount_change", F.when(F.expr("prev_amount != ."), F.lit("amount - prev_amount")).otherwise(F.lit("0")))
df_enriched_txns = df_enriched_txns.withColumn("tenure_group", F.when(F.expr("days_since_acq <= 90"), F.lit("NEW")).when(F.expr("days_since_acq <= 365"), F.lit("DEVELOPING")).when(F.expr("days_since_acq <= 730"), F.lit("MATURE")).when(F.expr("days_since_acq <= 365"), F.lit("DEVELOPING")).when(F.expr("days_since_acq <= 730"), F.lit("MATURE")).otherwise(F.lit("VETERAN")))

# RETAIN logic -- requires window function or iterative approach
# TODO: Manual review needed for RETAIN semantics
w = Window.partitionBy("customer_id", "txn_date").orderBy("customer_id", "txn_date")
df_enriched_txns = df_enriched_txns.withColumn("_row_num", F.row_number().over(w))
df_enriched_txns = df_enriched_txns.withColumn("_is_first", F.col("_row_num") == 1)
df_enriched_txns = df_enriched_txns.withColumn("_max_row", F.max("_row_num").over(w))
df_enriched_txns = df_enriched_txns.withColumn("_is_last", F.col("_row_num") == F.col("_max_row"))
df_enriched_txns = df_enriched_txns.drop("i")

# SAS ARRAY detected -- manual conversion required
# Consider using list comprehension with withColumn or UDF

df_enriched_txns.write.mode("overwrite").saveAsTable("glue_catalog.work.enriched_txns")


# PROC TRANSPOSE DATA=work.enriched_txns
df_enriched_txns = spark.table("glue_catalog.work.enriched_txns")
# PROC TRANSPOSE -- complex case, manual review needed
df_txns_long = df_enriched_txns  # TODO: implement transpose logic
df_txns_long = df_txns_long.withColumnRenamed("_NAME_", "metric")
df_txns_long = df_txns_long.withColumnRenamed("COL1", "value")
df_txns_long.write.mode("overwrite").saveAsTable("glue_catalog.work.txns_long")


# MACRO %run_all_months -- macro logic must be inlined or converted to Python function


# Unsupported: PROC_APPEND (line 78) -- manual conversion needed


#   PROC APPEND BASE=stage.yearly_txns DATA=work.enriched_txns;


#   RUN;


# CREATE TABLE mart.customer_summary
df_mart_customer_summary = spark.sql("""
SELECT customer_id,
           segment,
           region,
           tenure_group,
           SUM(amount) AS total_spend,
           COUNT(*) AS txn_count,
           MAX(txn_date) AS last_txn_date FORMAT=DATE9.
    FROM stage
    GROUP BY customer_id, segment, region, tenure_group
""")
df_mart_customer_summary.write.mode("overwrite").saveAsTable("glue_catalog.mart.customer_summary")



job.commit()
