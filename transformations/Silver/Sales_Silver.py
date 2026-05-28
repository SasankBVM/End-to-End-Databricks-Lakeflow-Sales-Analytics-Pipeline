import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Source View, fetches table from Bronze as source

@dlt.view(
    name = "sales_silver_view"
)
def sales_silver_view():

    df_sales = spark.readStream.table("sales_bronze")
    df_sales = df_sales.withColumn("pricePerSale", round(col("total_amount") /
                col("quantity"), 2))
    df_sales = df_sales.withColumn("processDate", current_timestamp())

    return df_sales

# Now we will define a Streaming table

dlt.create_streaming_table(
    name = "sales_silver_streaming_table"
)

dlt.create_auto_cdc_flow(
    source = "sales_silver_view",
    target = "sales_silver_streaming_table",
    keys= ["sales_id"],
    stored_as_scd_type = 1,
    sequence_by= col("processDate")
)




