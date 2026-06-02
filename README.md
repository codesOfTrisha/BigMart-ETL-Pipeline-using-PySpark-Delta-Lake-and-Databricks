# BigMart Databricks ETL Project

## Project Overview

This project demonstrates an end-to-end ETL pipeline built using Databricks, PySpark, and Delta Lake.

The objective is to process retail sales data using the Medallion Architecture.

## Architecture

Raw CSV
→ Bronze Layer
→ Silver Layer
→ Gold Layer

## Technologies Used

- Databricks
- PySpark
- Delta Lake
- GitHub

## Bronze Layer

- Read CSV data
- Validate schema
- Perform initial data quality checks

## Silver Layer

- Handle null values
- Remove duplicates
- Standardize text columns
- Create GST column
- Create Final_Price column

## Gold Layer

Business reports generated:

- Average MRP by Item Type
- Total Sales by Outlet Type
- Top 10 Expensive Products
- Product Counts by Category

## Key PySpark Operations

- select()
- filter()
- withColumn()
- fillna()
- groupBy()
- agg()
- orderBy()
- joins
- window functions

## Future Enhancements

- Delta Lake MERGE
- Databricks Workflows
- Automated Pipeline Scheduling
