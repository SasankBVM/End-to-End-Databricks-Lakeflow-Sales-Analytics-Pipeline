import dlt
from pyspark.sql.functions import *

# Gold streaming view on top of silver view (not silver table)
@dlt.view(
    name = "stores_gold_view"
)
def stores_gold_view():
    df = spark.readStream.table("stores_silver_view")
    return df

# Creating FACT table
dlt.create_streaming_table(
    name = "dim_stores"
)

dlt.create_auto_cdc_flow(
    source = "stores_gold_view",
    target = "dim_stores",
    keys= ["store_id"],
    stored_as_scd_type = 2,
    sequence_by= col("processed_date"),
    except_column_list=["processed_date"]
)