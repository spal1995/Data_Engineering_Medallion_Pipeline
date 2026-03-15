# Medallion architecture ETL pipeline using PySpark, Postgres SQL and Docker

## Project Overview

This project demonstrates an **end-to-end Data Engineering pipeline** built using the **Medallion Architecture (Bronze → Silver → Gold)**.

Raw CSV datasets are ingested from **AWS S3**, processed using **PySpark**, and stored in **PostgreSQL** across structured data warehouse layers.

The entire system runs inside **Docker containers**, creating a reproducible environment for building scalable data pipelines and preparing **analytics-ready datasets**.

The project illustrates how raw operational data can be transformed into structured datasets suitable for **analytics, machine learning, and business intelligence workflows**.

[!Data_layers](data_layers.pdf)

---

# Architecture

```
AWS S3 (Raw CSV Data)
        │
        ▼
PySpark Processing Engine
        │
        ▼
PostgreSQL Data Warehouse
 ├── Bronze Layer (Raw Data)
 ├── Silver Layer (Cleaned Data)
 └── Gold Layer (Business Ready Data)
        │
        ▼
Analytics / BI / ML Models
```

---

# Medallion Architecture

The project follows the **Medallion Data Architecture**, a modern data engineering design pattern used to progressively refine datasets.

| Layer  | Purpose                               |
| ------ | ------------------------------------- |
| Bronze | Raw ingestion of source datasets      |
| Silver | Cleaned, validated, standardized data |
| Gold   | Business-ready analytical datasets    |

This layered architecture improves **data reliability, maintainability, and scalability**.

---

# Tech Stack

| Component               | Technology |
| ----------------------- | ---------- |
| Data Processing         | PySpark    |
| Data Warehouse          | PostgreSQL |
| Containerization        | Docker     |
| Data Source             | AWS S3     |
| Development Environment | JupyterLab |
| Database Interface      | pgAdmin    |

---

# Key Data Engineering Skills Demonstrated

* End-to-end **ETL / ELT pipeline design**
* **Medallion data architecture implementation**
* Distributed data processing using PySpark
* Data ingestion from AWS S3
* Data cleaning and transformation pipelines
* Data warehouse schema design
* Containerized data engineering environments using Docker
* Analytics-ready dataset preparation

---

# Project Workflow

## 1. Docker Environment Setup

The entire data platform runs inside **Docker containers**.

The environment launches:

* PySpark container
* PostgreSQL database
* pgAdmin interface
* JupyterLab environment

This allows the project to run in a **fully reproducible environment**.

Start the system with:

```
docker-compose up -d
```

---

# 2. Data Ingestion – Bronze Layer

Raw CSV datasets stored in **AWS S3** are ingested using PySpark.

Steps:

1. Configure AWS credentials inside PySpark
2. Connect to S3 storage
3. Load raw CSV datasets
4. Store datasets in the **Bronze schema** of PostgreSQL

The Bronze layer preserves the **raw source data** for traceability and auditing.

---

# 3. Data Cleaning & Transformation – Silver Layer

The Silver layer performs **data preparation and standardization**.

Data quality processes include:

* Removing duplicate records
* Handling missing values
* Fixing inconsistent formats
* Standardizing field structures

Cleaned datasets are then stored in the **Silver schema**.

This layer ensures **validated and structured data** suitable for analysis.

---

# 4. Business Data Modeling – Gold Layer

The Gold layer creates **analytics-ready datasets**.

Transformations include:

* Table joins between entities
* Creation of surrogate keys
* Aggregated business views
* Analytical data structures

These datasets support **BI dashboards and machine learning workflows**.

---

# Project Structure

```
project-root
│
├── docker-compose.yml
├── docker_compose_information
│
├── data_Loading
│   └── pyspark_all_at_once_bronze_load.py
│
├── datasets
│   ├── crm
|   |     └── csv files
│   └── erp
│        └── csv files
│
├── medallion_Stages
│   ├── bronze
│   ├── silver
│   └── gold
│
└── README.md
```

---

# Running the Project

## Step 1: Start Docker Containers

```
docker-compose up -d
```

This launches:

* PySpark
* PostgreSQL
* pgAdmin

---

# Step 2: Open JupyterLab

Use the PySpark container to run notebooks that:

* Connect to AWS S3
* Load raw datasets
* Run PySpark transformations

Reference tutorial used:

https://www.youtube.com/watch?v=rBUbBPctj9s

---

# Step 3: Verify Data in pgAdmin

Open pgAdmin in your browser:

```
http://localhost:8085
```

Example credentials:

```
Email: admin@admin.com
Password: admin
```

---

# Configure Database Connection

### General Tab

```
Name: Medallion_DB
```

### Connection Tab

```
Host: pgdatabase
Port: 5432
Database: Medallion
Username: root
Password: root
```

Click **Save**.

---

# Explore the Medallion Schemas

```
Databases
   └── Medallion
        └── Schemas
             ├── bronze
             ├── silver
             └── gold
```

You can inspect:

* Raw ingestion tables
* Cleaned datasets
* Business-ready analytics views

---

# Example Analytics Query

Example query using the Gold layer:

```
SELECT customer_id,
       COUNT(order_id) AS total_orders,
       SUM(order_value) AS total_revenue
FROM gold.customer_sales_summary
GROUP BY customer_id
ORDER BY total_revenue DESC;
```

This demonstrates how the pipeline supports **business insights and analytics**.

---

# Data Quality Validation

To ensure reliable analytics datasets, validation checks can be added during the Silver stage.

Examples include:

* Null value validation
* Schema validation
* Duplicate detection
* Constraint enforcement

These checks improve **data reliability for analytics and machine learning pipelines**.

---

# Pipeline Monitoring & Logging

Future improvements include adding **pipeline observability** features such as:

* Execution logging
* Transformation metrics
* Failure alerts
* Data lineage tracking

These features are important for **production-grade data engineering pipelines**.

---

# MLOps & AI Integration Potential

The Gold layer datasets can serve as inputs for **machine learning pipelines** such as:

* Predictive models
* Customer segmentation
* Anomaly detection
* Recommendation systems

The architecture is designed to support **scalable ML workflows and data science experimentation**.

---

# Future Improvements

Possible enhancements include:

* Pipeline orchestration using Apache Airflow
* Incremental data ingestion strategies
* Automated data quality validation
* Monitoring and logging pipelines
* Integration with BI tools (Power BI / Tableau)
* CI/CD pipeline for automated deployments

---

# Why This Project Matters

Modern organizations require scalable pipelines that transform raw operational data into structured analytical datasets.

This project demonstrates how **cloud storage, distributed processing, and containerized infrastructure** can be combined to build reliable and scalable **data engineering pipelines**.

---

# Author

**Shubhrajit Pal**

Data Engineering project demonstrating an end-to-end pipeline using:

* AWS S3
* PySpark
* Docker
* PostgreSQL
* Medallion Data Architecture

GitHub
https://github.com/spal1995
