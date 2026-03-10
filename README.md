# Medallion Data Engineering Pipeline (PySpark + Docker + PostgreSQL)

## Overview

This project implements an **end-to-end Data Engineering pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)**.
The pipeline ingests raw CSV datasets from **AWS S3**, processes them using **PySpark**, and stores the results in **PostgreSQL** across different layers of the medallion architecture.

The entire environment runs in **Docker**, enabling a reproducible setup that includes:

* **PySpark**
* **PostgreSQL**
* **pgAdmin**
* **JupyterLab**

The pipeline demonstrates how raw data can be ingested, cleaned, standardized, and transformed into **analytics-ready datasets**.

---

## Architecture

Raw Data (CSV in AWS S3)
⬇
PySpark (Data Processing)
⬇
PostgreSQL Data Warehouse

Medallion Layers:

Bronze Layer → Raw ingestion
Silver Layer → Cleaned and standardized data
Gold Layer → Business-ready views for analytics

---

## Tech Stack

| Component               | Technology |
| ----------------------- | ---------- |
| Data Processing         | PySpark    |
| Data Warehouse          | PostgreSQL |
| Containerization        | Docker     |
| Data Source             | AWS S3     |
| Development Environment | JupyterLab |
| Database UI             | pgAdmin    |

---

## Project Workflow

### 1. Docker Environment Setup

The entire system is containerized using **Docker**.

The Docker setup launches:

* **PySpark container**
* **PostgreSQL database**
* **pgAdmin UI**

This ensures that all dependencies are isolated and the environment can be easily reproduced.

---

### 2. Data Ingestion (Bronze Layer)

Inside the **PySpark container**, JupyterLab is used to run PySpark notebooks.

Steps:

1. Configure **AWS credentials** inside the PySpark environment.
2. Access raw **CSV datasets stored in AWS S3**.
3. Use **PySpark** to read the datasets.
4. Load the raw datasets into the **Bronze schema in PostgreSQL**.

The Bronze layer represents **raw ingested data with minimal transformation**.

---

### 3. Data Cleaning & Standardization (Silver Layer)

Once the raw data is available in the Bronze layer:

1. Business data structures are analyzed.
2. Data quality issues are addressed:

   * duplicate records
   * inconsistent formats
   * missing values
3. Data is **cleaned and standardized** using PySpark transformations.

The processed data is then loaded into the **Silver schema**.

The Silver layer contains **validated and structured data suitable for downstream analysis**.

---

### 4. Data Modeling & Business Layer (Gold Layer)

In the Gold layer:

1. Relationships between tables are analyzed.
2. Relevant **joins** between entities are created.
3. **Surrogate keys** are introduced where required.
4. **Business views** are created for analytics and reporting.

The Gold layer provides **analytics-ready datasets optimized for BI and reporting tools**.

---

## Medallion Architecture

Bronze → Raw ingestion
Silver → Cleaned and standardized data
Gold → Business-ready views

| Layer  | Purpose                                       |
| ------ | --------------------------------------------- |
| Bronze | Raw data ingestion from AWS S3                |
| Silver | Data cleaning, validation, and transformation |
| Gold   | Aggregated and business-ready datasets        |

---

## Project Structure

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
│   └── crm └── erp
│        └── csv files
├── medallion_Stages
│   ├── bronze
│   ├── silver
│   └── gold
│
└── README.md
```

---

## How to Run the Project

### 1. Start the Docker Environment

```
docker-compose up -d
```

This launches:

* PySpark
* PostgreSQL
* pgAdmin

---

### 2. Open JupyterLab

Access JupyterLab from the PySpark container.

Run the PySpark notebooks to:

* connect to AWS S3 : watch https://www.youtube.com/watch?v=rBUbBPctj9s&list=PL2IsFZBGM_IExqZ5nHg0wbTeiWVd8F06b&index=5
* ingest raw datasets
* run transformations

---

### 3. Verify Data in PostgreSQL Using pgAdmin

The project includes **pgAdmin** to visually inspect the database tables and schemas.

#### Access pgAdmin

After running the Docker containers, open your browser and go to:

```
http://localhost:8085
```

Login using the credentials defined in the `docker-compose.yml` file.

Example:

```
Email: admin@admin.com
Password: admin
```

---

#### Create a Database Connection

Once logged into pgAdmin:

1. Click **Add New Server**
2. Go to the **General** tab
3. Enter a name for the connection (example: `Medallion_DB`)

---

#### Configure the Connection

Go to the **Connection** tab and enter:

```
Host name/address: pgdatabase
Port: 5432
Database: Medillion
Username: root
Password: root
```

These values correspond to the **PostgreSQL container configuration in Docker**.

Click **Save**.

---

#### Explore the Schemas

After connecting, expand the database to view the schemas:

```
Databases
   └── Medillion
        └── Schemas
             ├── bronze
             ├── silver
             └── gold
```

These schemas represent the **Medallion Architecture layers** used in this project.

You can inspect:

* tables loaded in the **Bronze layer**
* cleaned and standardized tables in the **Silver layer**
* business-ready views in the **Gold layer**

---

## Key Concepts Demonstrated

* Medallion Architecture
* Data ingestion from AWS S3
* Distributed data processing with PySpark
* Data cleaning and transformation
* Data modeling
* Surrogate keys
* Containerized data engineering environments

---

## Future Improvements

Potential enhancements include:

* Adding **Apache Airflow** for pipeline orchestration
* Implementing **incremental data loads**
* Adding **data quality validation checks**
* Integrating **BI tools for visualization**

---

## Author

Data Engineering Project demonstrating an end-to-end pipeline using AWS, PySpark, Docker, and PostgreSQL.
