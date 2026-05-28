import dlt
from pyspark.sql.functions import *

# Gold streaming view on top of silver view (not silver table)
@dlt.view(
    name = "products_gold_view"
)
def products_gold_view():
    df = spark.readStream.table("products_silver_view")
    return df

# Creating FACT table
dlt.create_streaming_table(
    name = "dim_products"
)

dlt.create_auto_cdc_flow(
    source = "products_gold_view",
    target = "dim_products",
    keys= ["product_id"],
    stored_as_scd_type = 2,
    sequence_by= col("processDate"),
    except_column_list=["processDate"]
)