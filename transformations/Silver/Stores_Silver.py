import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = "stores_silver_view"
)
def stores_silver_view():
    df_stores = spark.readStream.table("stores_bronze")
    df_stores = df_stores.withColumn("store_name", regexp_replace(col("store_name"), "_",""))
    df_stores = df_stores.withColumn("processed_date",current_date())
    return df_stores

dlt.create_streaming_table(
    name = "stores_silver_streaming_table"
)

dlt.create_auto_cdc_flow(
    source = "stores_silver_view",
    target = "stores_silver_streaming_table",
    keys = ["store_id"],
    stored_as_scd_type=1,
    sequence_by=col("processed_date")
)