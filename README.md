# BigMart ETL Pipeline using PySpark, Delta Lake & Databricks

## Project Overview

Designed and implemented an end-to-end ETL (Extract, Transform, Load) pipeline using Databricks, PySpark, and Delta Lake following the Medallion Architecture (Bronze → Silver → Gold).

The pipeline processes raw retail sales data from BigMart, performs data quality validation, standardization, feature engineering, audit tracking, and generates business-ready analytical datasets for reporting and decision-making.

This project simulates a real-world Data Engineering workflow by incorporating data cleansing, business transformations, window functions, Delta Lake storage, and KPI generation.

---

## Business Problem

Retail organizations generate large volumes of transactional data that often contain:

* Missing values
* Inconsistent formats
* Duplicate records
* Data quality issues

The objective of this project is to:

* Ingest raw sales data into a scalable data platform.
* Improve data quality through cleansing and standardization.
* Enrich data with business metrics and audit information.
* Generate curated datasets for business intelligence and reporting.
* Implement a Medallion Architecture for structured data processing.

---

## Architecture

```text
                Raw CSV Data
                       │
                       ▼
          ┌─────────────────────┐
          │     Bronze Layer    │
          │   Raw Data Storage  │
          └─────────────────────┘
                       │
                       ▼
          ┌─────────────────────┐
          │     Silver Layer    │
          │ Data Quality Checks │
          │ Data Transformations│
          └─────────────────────┘
                       │
                       ▼
          ┌─────────────────────┐
          │      Gold Layer     │
          │ Business Analytics  │
          │ KPI Generation      │
          └─────────────────────┘
                       │
                       ▼
              Reporting & Insights
```

---

## Technology Stack

| Technology             | Purpose                     |
| ---------------------- | --------------------------- |
| PySpark                | Distributed Data Processing |
| Databricks             | Data Engineering Platform   |
| Delta Lake             | Reliable Data Storage       |
| Python                 | Transformation Logic        |
| SQL                    | Data Analysis               |
| GitHub                 | Version Control             |
| Medallion Architecture | Data Layering Strategy      |

---

## Dataset Information

The project uses BigMart retail sales data containing:

* Product Information
* Outlet Information
* Sales Metrics
* Product Categories
* Outlet Characteristics

The dataset is stored as a CSV file and processed through multiple layers of transformation.

---

# Bronze Layer – Raw Data Ingestion

## Objective

Store raw source data without applying business transformations.

## Activities Performed

* Read CSV data from Databricks Volume
* Schema inference
* Header validation
* Initial data exploration
* Record count validation
* Schema inspection

## Key Functions Used

```python
spark.read.format("csv")
.option("header","true")
.option("inferSchema","true")
```

---

# Silver Layer – Data Cleaning & Transformation

## Objective

Improve data quality and enrich the dataset with business-ready attributes.

---

## Data Quality Checks

* Distinct value validation
* Duplicate record detection
* Schema validation

---

## Missing Value Handling

### Item_Weight

* Calculated average weight
* Replaced missing values using `fillna()`

### Outlet_Size

* Replaced missing values with `"Unknown"`

---

## Data Standardization

Standardized inconsistent categorical values in:

* Item_Fat_Content
* Product category text formatting

Examples:

| Original Value        | Standardized Value    |
| --------------------- | --------------------- |
| LF                    | Low Fat               |
| low fat               | Low Fat               |
| Low Fat               | Low Fat               |
| reg                   | Regular               |
| fruits and vegetables | Fruits And Vegetables |

### Functions Used

* when()
* otherwise()
* isin()
* initcap()

---

## Feature Engineering

Created the following business columns:

| Column           | Formula                                  |
| ---------------- | ---------------------------------------- |
| GST              | Item_MRP × 18%                           |
| Final_Price      | Item_MRP + GST                           |
| Outlet_Age       | Current Year - Outlet Establishment Year |
| Estimated_Profit | Item_Outlet_Sales × 20%                  |

---

## Business Categorization

### Profit_Category

Products classified into:

* High
* Medium
* Low

### Price_Band

Products segmented into:

* Budget
* Premium

### Sales_Category

Products classified into:

* Low
* Medium
* High

---

## Audit & Data Lineage

To support traceability and governance, audit columns were added:

| Column         | Purpose                      |
| -------------- | ---------------------------- |
| Load_Timestamp | Data Load Tracking           |
| Data_Source    | Source System Identification |

### Functions Used

```python
current_timestamp()
lit()
```

---

## Advanced Analytics

### Product Ranking using Window Functions

Implemented ranking logic to identify top-performing products within each outlet.

Generated:

* Sales_Rank
* Top_Seller Flag

### Window Functions Used

```python
rank()
Window.partitionBy()
orderBy()
```

---

## Validation Checks

Performed validation to ensure:

* Null values removed successfully
* Standardized values applied correctly
* Derived columns created successfully
* Schema integrity maintained

---

# Gold Layer – Business Analytics

## Objective

Create analytics-ready datasets and business KPIs.

---

## 1. Sales Analysis by Outlet Type

### Business Question

Which outlet type generates the highest revenue?

### Metrics

* Total Sales

---

## 2. Sales Analysis by Item Type

### Business Question

Which product category contributes the highest sales?

### Metrics

* Total Sales

---

## 3. Sales Analysis by Outlet Location

### Business Question

Which outlet location tier performs best?

### Metrics

* Total Sales

---

## 4. Outlet Performance Dashboard

### KPIs Generated

* Total Sales
* Average Sales
* Product Count

### Dimensions

* Outlet Identifier
* Outlet Type

---

## 5. Top Products Analysis

### Identified

* Top 10 Selling Products

### Metrics

* Total Sales

---

## 6. Profit Analysis

### Metrics

* Total Profit

### Grouped By

* Outlet Type

---

## 7. Executive KPI Dashboard

Generated enterprise-level KPIs:

* Total Sales
* Average Sales
* Total Profit
* Total Product Count

---

# Delta Lake Implementation

Processed datasets were stored as Delta Tables.

## Silver Layer

* BigMart_Silver

## Gold Layer

* Gold_Sales_By_Outlet_Type
* Gold_Outlet_Dashboard
* Gold_Top_Products
* Gold_KPI_Summary

### Benefits

* ACID Transactions
* Reliable Storage
* Faster Query Performance
* Scalable Data Processing

---

# Project Workflow

1. Ingest raw CSV data into Bronze Layer.
2. Validate schema and perform initial profiling.
3. Handle missing values and standardize categorical columns.
4. Create derived business columns.
5. Add audit and lineage tracking.
6. Apply window functions for advanced analytics.
7. Store cleansed data in Silver Delta Tables.
8. Generate aggregated KPI datasets in Gold Layer.
9. Store business-ready datasets as Delta Tables.
10. Enable reporting and dashboard consumption.

---

# PySpark Functions Used

## Aggregations

* sum()
* avg()
* count()

## Null Handling

* fillna()

## Conditional Logic

* when()
* otherwise()

## String Functions

* initcap()
* isin()

## Audit Functions

* current_timestamp()
* lit()

## Window Functions

* rank()
* Window.partitionBy()
* orderBy()

## DataFrame Operations

* select()
* withColumn()
* filter()
* groupBy()
* agg()
* distinct()

---

# Skills Demonstrated

## Data Engineering

* ETL Pipeline Development
* Medallion Architecture
* Data Quality Framework
* Data Lineage
* Audit Tracking

## PySpark

* DataFrame Operations
* Aggregations
* Feature Engineering
* Window Functions
* Data Cleansing

## Databricks

* Notebook Development
* Delta Lake Integration
* Data Pipeline Design

## Analytics

* KPI Generation
* Business Reporting
* Sales Analytics
* Profitability Analysis

---

# Key Project Achievements

* Built a complete end-to-end ETL pipeline using Databricks and PySpark.
* Implemented Bronze, Silver, and Gold architecture.
* Improved data quality through validation and standardization.
* Created business-focused KPIs and analytical datasets.
* Leveraged Delta Lake for reliable and scalable storage.
* Applied window functions for advanced analytical use cases.
* Added audit and lineage tracking to support enterprise data governance.

---

# Future Enhancements

* Incremental Data Loading
* Delta Lake MERGE Operations
* Databricks Workflows Scheduling
* Automated Data Quality Monitoring
* CI/CD Integration with GitHub Actions
* Real-Time Streaming Data Processing

---

# Project Summary

Designed and implemented an end-to-end retail sales ETL pipeline using Databricks, PySpark, and Delta Lake following the Medallion Architecture. The solution ingests raw BigMart sales data, performs data quality validation, standardization, feature engineering, and audit tracking, and generates Gold-layer business KPIs including outlet performance, sales analysis, product rankings, profitability insights, and executive dashboards.

The project demonstrates practical Data Engineering skills including data ingestion, transformation, Delta Lake storage, window functions, audit lineage, and KPI generation.
