import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import *


# Source View, fetches table from Bronze as source

@dlt.view(
    name = "customers_silver_view"
)
def customers_silver_view():

    df_customers = spark.readStream.table("customers_bronze")
    df_customers = df_customers.withColumn("name", upper(col("name")))
    df_customers = df_customers.withColumn("domain", split(col ("email"), "@")[1])
    df_customers = df_customers.withColumn("processDate", current_timestamp())

    return df_customers

# Now we will define a Streaming table

dlt.create_streaming_table(
    name = "customer_silver_streaming_table"
)

dlt.create_auto_cdc_flow(
    source = "customers_silver_view",
    target = "customer_silver_streaming_table",
    keys= ["customer_id"],
    stored_as_scd_type = 1,
    sequence_by= col("processDate")
)




