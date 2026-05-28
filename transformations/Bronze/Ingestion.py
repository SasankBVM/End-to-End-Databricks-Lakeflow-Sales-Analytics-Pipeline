import dlt


# Ingesting data

@dlt.table(
    name = "sales_bronze"
)
def sales_bronze():
    df = spark.readStream.format("cloudFiles").\
        option("cloudFiles.format", "csv").\
        load("/Volumes/databricks_project/bronze/schema_volume/Sales/")
    return df


@dlt.table(
    name = "customers_bronze"
)
def customers_bronze():
    df = spark.readStream.format("cloudFiles").\
        option("cloudFiles.format", "csv").\
        load("/Volumes/databricks_project/bronze/schema_volume/Customers/")
    return df


@dlt.table(
    name = "products_bronze"
)
def products_bronze():
    df = spark.readStream.format("cloudFiles").\
        option("cloudFiles.format", "csv").\
        load("/Volumes/databricks_project/bronze/schema_volume/Products/")
    return df


@dlt.table(
    name = "stores_bronze"
)
def stores_bronze():
    df = spark.readStream.format("cloudFiles").\
        option("cloudFiles.format", "csv").\
        load("/Volumes/databricks_project/bronze/schema_volume/Stores/")
    return df


