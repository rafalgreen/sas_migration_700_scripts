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
# Source: sample_simple.sas
# Converted: 2026-03-10T12:35:43.953720+00:00
# Tier: GREEN
# ---------------------------------------------------------------------------


# DATA work.customers_filtered; SET mylib.customers;
df_mylib_customers = spark.table("glue_catalog.mylib.customers")
df_customers_filtered = df_mylib_customers
df_customers_filtered = df_customers_filtered.filter("age >= 18 AND status = 'ACTIVE'")
df_customers_filtered = df_customers_filtered.select("customer_id", "name", "age", "status", "region")

df_customers_filtered.write.mode("overwrite").saveAsTable("glue_catalog.work.customers_filtered")


# PROC SORT DATA=work.customers_filtered
df_customers_filtered = spark.table("glue_catalog.work.customers_filtered")
df_customers_filtered = df_customers_filtered.orderBy(F.col("region").asc(), F.col("customer_id").asc())
df_customers_filtered.write.mode("overwrite").saveAsTable("glue_catalog.work.customers_filtered")


# CREATE TABLE work.region_summary
df_region_summary = spark.sql("""
SELECT region,
           COUNT(*) AS customer_count,
           AVG(age) AS avg_age
    FROM work
    GROUP BY region
""")
df_region_summary.write.mode("overwrite").saveAsTable("glue_catalog.work.region_summary")



job.commit()
