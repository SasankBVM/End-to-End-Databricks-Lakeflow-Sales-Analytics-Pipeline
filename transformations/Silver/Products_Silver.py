import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Source View, fetches table from Bronze as source

@dlt.view(
    name = "products_silver_view"
)
def products_silver_view():

    df_products = spark.readStream.table("products_bronze")
    df_products = df_products.withColumn("processDate", current_timestamp())

    return df_products

# Now we will define a Streaming table

dlt.create_streaming_table(
    name = "products_silver_streaming_table"
)

dlt.create_auto_cdc_flow(
    source = "products_silver_view",
    target = "products_silver_streaming_table",
    keys= ["product_id"],
    stored_as_scd_type = 1,
    sequence_by= col("processDate")
)




