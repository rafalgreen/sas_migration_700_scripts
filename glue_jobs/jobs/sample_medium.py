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
# Source: sample_medium.sas
# Converted: 2026-03-10T12:35:43.953429+00:00
# Tier: YELLOW
# ---------------------------------------------------------------------------


# %INCLUDE '/macros/date_utils.sas' -- import equivalent module


# MERGE sales.orders ref.products; BY product_id;
df_sales_orders = spark.table("glue_catalog.sales.orders")
df_ref_products = spark.table("glue_catalog.ref.products")
df_orders_enriched = df_sales_orders.join(df_ref_products, on=["product_id"], how="inner")
df_orders_enriched.write.mode("overwrite").saveAsTable("glue_catalog.work.orders_enriched")


# PROC SORT DATA=work.orders_enriched
df_orders_enriched = spark.table("glue_catalog.work.orders_enriched")
df_orders_enriched = df_orders_enriched.orderBy(F.col("customer_id").asc(), F.col("order_date").asc())
df_orders_enriched.write.mode("overwrite").saveAsTable("glue_catalog.work.orders_enriched")


# DATA work.customer_running_total; SET work.orders_enriched;
df_orders_enriched = spark.table("glue_catalog.work.orders_enriched")
df_customer_running_total = df_orders_enriched
df_customer_running_total = df_customer_running_total.withColumn("running_total", F.when(F.expr("FIRST.customer_id"), F.lit("0")))

# RETAIN logic -- requires window function or iterative approach
# TODO: Manual review needed for RETAIN semantics
w = Window.partitionBy("customer_id").orderBy("customer_id")
df_customer_running_total = df_customer_running_total.withColumn("_row_num", F.row_number().over(w))
df_customer_running_total = df_customer_running_total.withColumn("_is_first", F.col("_row_num") == 1)
df_customer_running_total = df_customer_running_total.withColumn("_max_row", F.max("_row_num").over(w))
df_customer_running_total = df_customer_running_total.withColumn("_is_last", F.col("_row_num") == F.col("_max_row"))

df_customer_running_total.write.mode("overwrite").saveAsTable("glue_catalog.work.customer_running_total")


# PROC MEANS / SUMMARY DATA=work.orders_enriched
df_orders_enriched = spark.table("glue_catalog.work.orders_enriched")
df_tier_stats = df_orders_enriched.groupBy("price_tier").agg(
    F.mean("total_amount").alias("avg_amount"),
    F.sum("quantity").alias("total_qty"),
    F.count("total_amount").alias("order_count")
)
df_tier_stats.write.mode("overwrite").saveAsTable("glue_catalog.work.tier_stats")


job.commit()
