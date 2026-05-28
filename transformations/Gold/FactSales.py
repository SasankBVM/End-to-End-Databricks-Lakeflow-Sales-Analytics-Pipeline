import dlt
from pyspark.sql.functions import *

# Gold streaming view on top of silver view (not silver table)
@dlt.view(
    name = "sales_gold_view"
)
def sales_gold_view():
    df = spark.readStream.table("sales_silver_view")
    return df

# Creating FACT table
dlt.create_streaming_table(
    name = "fact_sales"
)

dlt.create_auto_cdc_flow(
    source = "sales_gold_view",
    target = "fact_sales",
    keys= ["sales_id"],
    stored_as_scd_type = 1,
    sequence_by= col("processDate")
)