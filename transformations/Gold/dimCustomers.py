import dlt
from pyspark.sql.functions import *

# Gold streaming view on top of silver view (not silver table)
@dlt.view(
    name = "customers_gold_view"
)
def customers_gold_view():
    df = spark.readStream.table("customers_silver_view")
    return df

# Creating FACT table
dlt.create_streaming_table(
    name = "dim_customers"
)

dlt.create_auto_cdc_flow(
    source = "customers_silver_view",
    target = "dim_customers",
    keys= ["customer_id"],
    stored_as_scd_type = 2,
    sequence_by= col("processDate"),
    except_column_list=["processDate"]
)