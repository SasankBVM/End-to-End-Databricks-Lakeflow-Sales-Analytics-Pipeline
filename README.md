# E2E_Pipeline - End-to-End Data Pipeline

A production-ready data engineering pipeline built on Databricks using **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables). This pipeline implements the **Medallion Architecture** (Bronze → Silver → Gold) for processing sales, customer, product, and store data with automated change data capture (CDC) and slowly changing dimensions (SCD).

## 🏗️ Architecture Overview

This pipeline follows the **Medallion Architecture** pattern, a data design approach that logically organizes data in a lakehouse:

```
┌─────────────────────────────────────────────────────────────────┐
│                         SOURCE DATA                              │
│  Unity Catalog Volumes: /Volumes/databricks_project/bronze/...  │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                          │
│  • sales_bronze      • customers_bronze                          │
│  • products_bronze   • stores_bronze                             │
│  ✓ Auto Loader (streaming ingestion)                            │
│  ✓ Schema inference & evolution                                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleaned)                        │
│  • sales_silver_streaming_table                                 │
│  • customer_silver_streaming_table                              │
│  • products_silver_streaming_table                              │
│  • stores_silver_streaming_table                                │
│  ✓ Data quality & transformations                               │
│  ✓ AUTO CDC (SCD Type 1)                                        │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Curated)                         │
│  • fact_sales (SCD Type 1)                                       │
│  • dim_customers (SCD Type 2 with history)                       │
│  • dim_products (SCD Type 2 with history)                        │
│  • dim_stores (SCD Type 2 with history)                          │
│  ✓ Business-ready dimensional model                             │
│  ✓ Optimized for analytics & BI                                 │
└─────────────────────────────────────────────────────────────────┘
```

## ✨ Features

* **Medallion Architecture**: Progressive data refinement through Bronze, Silver, and Gold layers
* **Streaming Data Ingestion**: Auto Loader for incremental file processing with schema evolution
* **Change Data Capture**: Automated CDC flows with SCD Type 1 and Type 2 support
* **Data Quality**: Built-in validation and transformation rules
* **Serverless Compute**: Auto-scaling, no cluster management required
* **Photon Acceleration**: High-performance query engine enabled
* **Unity Catalog Integration**: Secure, governed data access
* **Dimensional Modeling**: Star schema with fact and dimension tables

## 📁 Directory Structure

```
E2E_Pipeline/
├── transformations/
│   ├── Bronze/
│   │   └── Ingestion.py              # Auto Loader ingestion from UC volumes
│   ├── Silver/
│   │   ├── Sales_Silver.py           # Sales transformations + SCD Type 1
│   │   ├── Customers_Silver.py       # Customer transformations + SCD Type 1
│   │   ├── Products_Silver.py        # Product transformations + SCD Type 1
│   │   └── Stores_Silver.py          # Store transformations + SCD Type 1
│   ├── Gold/
│   │   ├── FactSales.py              # Sales fact table (SCD Type 1)
│   │   ├── dimCustomers.py           # Customer dimension (SCD Type 2)
│   │   ├── dimProducts.py            # Product dimension (SCD Type 2)
│   │   └── dimStores.py              # Store dimension (SCD Type 2)
│   └── my_transformation.sql         # Custom SQL transformations
├── explorations/                      # Ad-hoc analysis notebooks
└── README.md                          # This file
```

## 🔧 Prerequisites

* **Databricks Workspace** (AWS, Azure, or GCP)
* **Unity Catalog** enabled
* **Catalog**: `databricks_project`
* **Schema**: `gold`
* **Source Data Volumes**:
  * `/Volumes/databricks_project/bronze/schema_volume/Sales/`
  * `/Volumes/databricks_project/bronze/schema_volume/Customers/`
  * `/Volumes/databricks_project/bronze/schema_volume/Products/`
  * `/Volumes/databricks_project/bronze/schema_volume/Stores/`
* **Permissions**: `CREATE TABLE` on schema, `READ VOLUME` on source volumes

## 🚀 Setup Instructions

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd E2E_Pipeline
```

### 2. Import to Databricks Workspace
```bash
# Using Databricks CLI
databricks workspace import_dir \
  ./transformations \
  /Workspace/Users/<your-email>/E2E_Pipeline/transformations \
  --overwrite
```

Or manually:
* Navigate to Databricks workspace
* Create folder: `/Workspace/Users/<your-email>/E2E_Pipeline`
* Import all Python files from the repository

### 3. Create the Pipeline
1. Go to **Workflows** → **Lakeflow Pipelines**
2. Click **Create Pipeline**
3. Configure:
   * **Name**: `E2E_Pipeline`
   * **Pipeline Mode**: Triggered
   * **Catalog**: `databricks_project`
   * **Schema**: `gold`
   * **Source Code**: Glob pattern `/Workspace/Users/<your-email>/E2E_Pipeline/transformations/**`
   * **Serverless**: Enabled
   * **Photon**: Enabled
   * **Channel**: Current

### 4. Configure Source Data
Ensure CSV files are uploaded to Unity Catalog volumes:
```python
# Upload sample data to volumes
dbutils.fs.cp("local_path/sales.csv", 
              "/Volumes/databricks_project/bronze/schema_volume/Sales/")
```

### 5. Run the Pipeline
* Click **Start** in the pipeline UI
* Monitor progress in real-time
* View lineage and data quality metrics

## ⚙️ Pipeline Configuration

| Configuration | Value | Description |
| --- | --- | --- |
| **Pipeline ID** | `77746be2-c1fa-496f-a85a-105c0fa44f42` | Unique identifier |
| **Mode** | Triggered (Batch) | Runs on-demand or scheduled |
| **Serverless** | Enabled | Auto-scaling compute |
| **Photon** | Enabled | Accelerated query engine |
| **Channel** | Current | Latest stable features |
| **Catalog** | `databricks_project` | Unity Catalog namespace |
| **Target Schema** | `gold` | Output tables location |
| **Development** | Disabled | Production mode |

## 🔄 Data Flow Details

### Bronze Layer: Raw Data Ingestion
**File**: `transformations/Bronze/Ingestion.py`

Ingests raw CSV files using **Auto Loader** (cloudFiles) with streaming:

* **sales_bronze**: Sales transactions
* **customers_bronze**: Customer master data
* **products_bronze**: Product catalog
* **stores_bronze**: Store locations

**Key Features**:
* Automatic schema inference
* Schema evolution handling
* Incremental file processing
* Fault tolerance with checkpoints

### Silver Layer: Cleaned & Validated Data
**Files**: `Sales_Silver.py`, `Customers_Silver.py`, `Products_Silver.py`, `Stores_Silver.py`

Applies business logic and data quality rules:

#### Sales Silver (`Sales_Silver.py`)
* Calculates `pricePerSale = total_amount / quantity`
* Adds `processDate` timestamp
* **AUTO CDC**: Upserts based on `sales_id` (SCD Type 1)

#### Customers Silver (`Customers_Silver.py`)
* Converts customer names to uppercase
* Extracts email domain: `domain = split(email, '@')[1]`
* Adds `processDate` timestamp
* **AUTO CDC**: Upserts based on `customer_id` (SCD Type 1)

#### Products Silver (`Products_Silver.py`)
* Product-specific transformations
* **AUTO CDC**: Maintains latest product information

#### Stores Silver (`Stores_Silver.py`)
* Store-specific transformations
* **AUTO CDC**: Maintains latest store information

**CDC Configuration**:
```python
dlt.create_auto_cdc_flow(
    source = "customers_silver_view",
    target = "customer_silver_streaming_table",
    keys = ["customer_id"],
    stored_as_scd_type = 1,  # Overwrite on change
    sequence_by = col("processDate")
)
```

### Gold Layer: Analytics-Ready Data Model
**Files**: `FactSales.py`, `dimCustomers.py`, `dimProducts.py`, `dimStores.py`

Implements a **star schema** with fact and dimension tables:

#### Fact Table: `fact_sales` (`FactSales.py`)
* Central fact table for sales transactions
* **SCD Type 1**: Latest values only
* Keys: `sales_id`
* Measures: quantity, total_amount, pricePerSale

#### Dimension Table: `dim_customers` (`dimCustomers.py`)
* Customer attributes with **full history tracking**
* **SCD Type 2**: Maintains historical records
* Automatically adds:
  * `__START_AT`: Timestamp when record became active
  * `__END_AT`: Timestamp when record was superseded
  * `__CURRENT`: Boolean flag for current record
* Excludes `processDate` from change detection

**SCD Type 2 Configuration**:
```python
dlt.create_auto_cdc_flow(
    source = "customers_silver_view",
    target = "dim_customers",
    keys = ["customer_id"],
    stored_as_scd_type = 2,  # Historical tracking
    sequence_by = col("processDate"),
    except_column_list = ["processDate"]  # Exclude from change detection
)
```

#### Dimension Tables: `dim_products`, `dim_stores`
* Product and store attributes with historical tracking
* **SCD Type 2**: Same pattern as `dim_customers`

## 🛠️ Technologies Used

* **Lakeflow Spark Declarative Pipelines**: Declarative ETL framework
* **Auto Loader**: Incremental file ingestion with schema evolution
* **AUTO CDC**: Built-in change data capture with SCD Type 1 & 2
* **Unity Catalog**: Unified governance for data and AI assets
* **Delta Lake**: ACID transactions, time travel, schema enforcement
* **Apache Spark**: Distributed data processing
* **Photon**: Native vectorized query engine
* **PySpark**: Python API for Spark

## 📊 Usage & Monitoring

### Running the Pipeline
```python
# Programmatically trigger pipeline update
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.pipelines.start_update(
    pipeline_id="77746be2-c1fa-496f-a85a-105c0fa44f42",
    full_refresh=False  # Incremental update
)
```

### Querying Gold Tables
```sql
-- Query fact table with dimension lookups
SELECT 
    f.sales_id,
    f.quantity,
    f.total_amount,
    c.name AS customer_name,
    p.product_name,
    s.store_name
FROM databricks_project.gold.fact_sales f
LEFT JOIN databricks_project.gold.dim_customers c ON f.customer_id = c.customer_id AND c.__CURRENT = true
LEFT JOIN databricks_project.gold.dim_products p ON f.product_id = p.product_id AND p.__CURRENT = true
LEFT JOIN databricks_project.gold.dim_stores s ON f.store_id = s.store_id AND s.__CURRENT = true
WHERE f.sale_date >= '2024-01-01';
```

### Historical Queries (SCD Type 2)
```sql
-- View customer history over time
SELECT 
    customer_id,
    name,
    email,
    domain,
    __START_AT AS valid_from,
    __END_AT AS valid_to,
    __CURRENT AS is_current
FROM databricks_project.gold.dim_customers
WHERE customer_id = '12345'
ORDER BY __START_AT DESC;
```

### Monitoring
* **Pipeline UI**: Real-time progress, lineage, data quality metrics
* **Event Log**: Detailed execution logs and error messages
* **Data Quality Metrics**: Auto-generated stats on row counts, data freshness
* **System Tables**: Query `system.lakeflow.pipelines.update_logs` for historical runs

## 🔍 Troubleshooting

| Issue | Solution |
| --- | --- |
| **Schema evolution failures** | Enable `cloudFiles.schemaEvolutionMode = "addNewColumns"` in Auto Loader |
| **CDC key violations** | Verify unique keys in source data, check sequence_by column |
| **Volume access denied** | Grant `READ VOLUME` permission: `GRANT READ VOLUME ON VOLUME databricks_project.bronze.schema_volume TO <user>` |
| **Pipeline stuck in RUNNING** | Check for streaming query issues, review event logs |
| **Data quality failures** | Review expectations and add `.with_expectations()` decorators |

## 📈 Performance Optimization

* **Liquid Clustering**: Auto-enabled for optimized data layout
* **Predictive I/O**: Intelligent data prefetching
* **Photon Engine**: 3-5x faster query performance
* **Serverless Compute**: Instant auto-scaling, no warm-up time
* **Z-Ordering**: Automatically applied for frequently filtered columns

## 🔒 Security & Governance

* **Unity Catalog**: Centralized access control and auditing
* **Table ACLs**: Fine-grained permissions at catalog/schema/table level
* **Data Lineage**: Automatic tracking from source to consumption
* **Audit Logs**: Complete audit trail for compliance
* **Column-Level Encryption**: Supported through Unity Catalog

## 📚 Additional Resources

* [Lakeflow Pipelines Documentation](https://docs.databricks.com/en/lakeflow/index.html)
* [AUTO CDC Guide](https://docs.databricks.com/en/lakeflow/cdc.html)
* [Slowly Changing Dimensions](https://docs.databricks.com/en/lakeflow/scd.html)
* [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
* [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## 👥 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -m 'Add new feature'`
4. Push to branch: `git push origin feature/my-feature`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📧 Contact

For questions or support, please contact: manisasank911@gmail.com

---

**Last Updated**: May 2026  
**Pipeline Version**: 1.0  
**Databricks Runtime**: Current Channel (Serverless)
