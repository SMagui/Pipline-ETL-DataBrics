# Databricks ETL Pipeline – E-Commerce Sales

This project demonstrates a full-scale ETL pipeline using **Databricks**, **PySpark**, and **Delta Lake** following the **Bronze-Silver-Gold architecture**. It processes real-world e-commerce transactions from the [Online Retail II dataset](https://archive.ics.uci.edu/ml/datasets/Online+Retail+II).

##  Dataset
- **Source:** UCI Machine Learning Repository
- **Format:** Excel (.xlsx)
- **Fields:** Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer_ID, Country

##  Architecture
- **Bronze Layer:** Raw ingestion + audit timestamp
- **Silver Layer:** Cleaning, deduplication, type casting, enrichment
- **Gold Layer:** Daily sales metrics (total sales, item count, AOV)

## Technologies Used
- Databricks (Community Edition)
- PySpark / Spark SQL
- Delta Lake (Time Travel, ZORDER, Schema Enforcement)
- Git + GitHub

## Folder Structure
project-root/
├── 01_bronze_ingestion.py
├── 02_silver_cleaning.py
├── 03_gold_aggregation.py
├── data/
│ └── online_retail_II.xlsx 
└── README.md


## Gold Metrics Output

- `TotalSales`: Sum of revenue per day
- `TotalItems`: Items sold per day
- `AvgOrderValue`: Revenue ÷ Invoice count

## How to Run
- Import notebooks into Databricks
- Upload Excel data to managed volume
- Run notebooks in sequence: Bronze → Silver → Gold

##  Author
Built by **Tufan Bhattarai** as part of a professional data engineering demo.

