# 🎵 Spotify End-to-End Azure Data Engineering Project

![Azure](https://img.shields.io/badge/Microsoft_Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=databricks&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-1B3139?style=for-the-badge&logo=databricks&logoColor=white)

## 📌 Project Overview

An end-to-end, production-style **Azure Data Engineering** pipeline built on a Spotify streaming dataset. The project implements the **Medallion Architecture (Bronze → Silver → Gold)** to ingest, cleanse, govern, and model streaming and user data into an analytics-ready **Star Schema**.

Key engineering patterns demonstrated in this repository include **dynamic metadata-driven ingestion**, **JSON-based CDC watermarking**, **backdated refresh (backfilling)**, **Spark Structured Streaming with Auto Loader**, **Jinja2-powered dynamic SQL generation**, **Unity Catalog governance**, and **Slowly Changing Dimensions (SCD Type 2)** using **Lakeflow Declarative Pipelines (Delta Live Tables)**.

---

## 🏗️ System Architecture

<img width="1331" height="686" alt="image" src="https://github.com/user-attachments/assets/0451176a-6c8b-43a2-ba76-ac679a8073da" />

### Data Flow Summary
1. **Source:** Cloud-hosted **Azure SQL Database** storing operational Spotify tables (`DimUser`, `DimArtist`, `DimTrack`, `DimDate`, `FactStream`).
2. **Bronze Layer (Raw Ingestion):** **Azure Data Factory (ADF)** dynamically ingests incremental data into **ADLS Gen2** as timestamped Parquet files using JSON watermarks, automated empty-file cleanup, and **Azure Logic Apps** failure alerting.
3. **Silver Layer (Enriched & Conformed):** **Azure Databricks** processes incremental files via **Auto Loader (Spark Structured Streaming)**, applies modular PySpark transformations using custom Python utilities, writes **Delta Tables** governed by **Unity Catalog**, and enables dynamic business views using **Jinja2**.
4. **Gold Layer (Curated Star Schema):** **Lakeflow Declarative Pipelines (DLT)** builds the final dimensional model with **Auto CDC (SCD Type 2 for Dimensions, SCD Type 1 Upserts for Facts)** and built-in **Data Quality Expectations**.

---

## Resource Group

<img width="1831" height="995" alt="image" src="https://github.com/user-attachments/assets/7c091842-53c9-41ba-a562-445df86bd4c8" />

## Containers

<img width="1483" height="431" alt="image" src="https://github.com/user-attachments/assets/28f29762-8a86-4b24-bf28-2ddb9c628acb" />


## 🛠️ Tech Stack

| Category | Technology / Service | Purpose |
| :--- | :--- | :--- |
| **Cloud Platform** | Microsoft Azure | Cloud infrastructure and resource management |
| **Data Source** | Azure SQL Database | Transactional source system for initial and incremental loads |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS Gen2) | Hierarchical storage for `bronze`, `silver`, `gold`, and metastore containers |
| **Orchestration & Ingestion** | Azure Data Factory (ADF) | Dynamic, parameterized incremental pipelines with backfilling |
| **Alerting & Monitoring** | Azure Logic Apps | Automated email notifications on pipeline failures |
| **Compute & Processing** | Azure Databricks (Serverless), PySpark | Distributed streaming transformations and custom utilities |
| **Storage Format** | Parquet (Bronze), Delta Lake (Silver & Gold) | ACID transactions, schema evolution, and time travel |
| **Governance & Security** | Unity Catalog, Access Connector | Centralized metastore, storage credentials, and external locations |
| **Declarative ETL** | Lakeflow Declarative Pipelines (DLT) | Auto CDC (SCD Type 1 & 2) and data quality expectations |
| **Templating & Version Control** | Jinja2, GitHub | Metadata-driven SQL generation and Git CI/CD branching |

---

## 🥉 Bronze Layer: Dynamic Ingestion & Orchestration (ADF)

The Bronze layer focuses on reliable, reusable ingestion from Azure SQL Database to ADLS Gen2 (`bronze` container) without hardcoding table-specific pipelines.

### Key Features Implemented
* **Single Parameterized Loop Pipeline (`Incremental_Loop`):** Uses a `ForEach` activity driven by a JSON array parameter (`loop_input`) containing `schema`, `table`, `cdc_col`, and `from_date`. Onboarding a new table requires only adding one JSON entry.
* **Dynamic Datasets:** Parameterized `JSON_Dynamic` and `Parquet_Dynamic` datasets eliminate dataset sprawl across tables.
* **Modern JSON-Based Watermarking (CDC):** Each table maintains an independent `<table_name>_cdc/cdc.json` file (initialized to `1900-01-01`). After every successful load, a `Script` activity calculates `MAX(cdc_col)` and updates the JSON watermark.
* **Dynamic Backfilling (Backdated Refresh):** Passing a `from_date` parameter dynamically overrides the CDC watermark at runtime, enabling historical reloads for any specific date range without modifying pipeline code.
* **Empty-File Cleanup Optimization:** An `If Condition` evaluates `dataRead > 0`. If zero new records are found, the pipeline skips watermark updates and triggers a `Delete` activity to purge the empty Parquet file—preventing small-file clutter in ADLS Gen2.
* **Automated Failure Alerting:** Integrated an **Azure Logic Apps** HTTP POST endpoint via an ADF `Web` activity to send Outlook email alerts containing `PipelineName` and `PipelineRunId` upon pipeline failure.
* **Git CI/CD Workflow:** Configured ADF with GitHub using feature branches, Pull Requests to `main`, and automated ARM template publishing (`adf_publish`).

## ADF Incremental Loop Pipeline

<img width="1841" height="973" alt="image" src="https://github.com/user-attachments/assets/e631f914-46a5-44f2-86a2-4c310362961d" />

### Logic Apps 

<img width="1836" height="1006" alt="image" src="https://github.com/user-attachments/assets/7decf998-88e4-4cbe-a908-5bdfd12c0992" />

### Incremental ingestion

<img width="1793" height="991" alt="image" src="https://github.com/user-attachments/assets/30472c4d-242e-4093-b11c-3161d17fe85a" />

### Bronze Data 

<img width="1457" height="760" alt="image" src="https://github.com/user-attachments/assets/8a57f56f-deb8-433c-8de7-f45837e3917b" />
<img width="1453" height="417" alt="image" src="https://github.com/user-attachments/assets/2a2277b9-a4b6-4de6-810e-88b102d2eea0" />


---

## 🔐 Data Governance: Unity Catalog Setup

Before transforming data in Databricks, enterprise-grade access control and governance were configured from scratch:
* Created a **Databricks Access Connector** and assigned it the **Storage Blob Data Contributor** role on ADLS Gen2.
* Provisioned a dedicated **Unity Catalog Metastore** backed by a `databricks-metastore` container.
* Configured **Storage Credentials** and **External Locations** for `bronze`, `silver`, and `gold` containers.
* Structured the three-level namespace under catalog `spotify_cata` (`spotify_cata.silver.*` and `spotify_cata.gold.*`).

---

## 🥈 Silver Layer: Streaming Transformations & Metadata-Driven SQL

The Silver layer cleanses, deduplicates, and standardizes raw Bronze data into **Delta Tables** using **Azure Databricks** and **PySpark**.

### Key Features Implemented
* **Spark Structured Streaming & Auto Loader (`cloudFiles`):** Incrementally reads new Parquet files from ADLS Gen2 with built-in **Schema Evolution** (`_rescued_data`) and **Checkpointing** (RocksDB state management) to guarantee **idempotency (exactly-once processing)**.
* **Modular Custom Utilities (`utils/transformations.py`):** Built a reusable Python OOP class (`reusable`) dynamically imported via `sys.path` to standardize common operations such as multi-column dropping (`dropColumns`) across notebooks.
* **Entity-Specific Transformations:**
  * **`DimUser`:** Standardized `user_name` to uppercase, deduplicated on `user_id`, and dropped `_rescued_data`.
  * **`DimArtist`:** Deduplicated on `artist_id` and removed metadata rescue columns.
  * **`DimTrack`:** Engineered a `durationFlag` column (`low` < 150s, `medium` 150–300s, `high` > 300s) using `when().otherwise()`, and sanitized `track_name` by replacing hyphens (`-`) with spaces via `regexp_replace`.
  * **`DimDate` & `FactStream`:** Cleaned schemas and persisted streams with `trigger(once=True)` into Unity Catalog Delta tables.
* **Metadata-Driven Dynamic SQL Views (Jinja2):** Developed a reusable **Jinja2** templating notebook that accepts a structured metadata configuration (tables, aliases, columns, and join conditions) and dynamically compiles and executes multi-table `LEFT JOIN` Spark SQL queries—accelerating ad-hoc business view delivery without writing repetitive SQL.

## Silver notebook

https://github.com/chaghaas/azureproject/blob/d41cd7ac4a10a95e3c5d5942e7be735dbcc23169/AzureProject%20-%20databricks/spotify_dab/src/silver/silver_Dimensions.ipynb

## Jinja

https://github.com/chaghaas/azureproject/blob/da127c8c995789d395bd18feface8af1921cb198/AzureProject%20-%20databricks/spotify_dab/Jinja/jinja_notebook.ipynb

---

## 🥇 Gold Layer: Star Schema, SCD Type 2 & Data Quality (DLT)

The Gold layer delivers an analytics-ready **Star Schema** (`FactStream` joined with `DimUser`, `DimTrack`, and `DimDate`) using **Lakeflow Declarative Pipelines (Delta Live Tables)**.

### Key Features Implemented
* **Staging Layer:** Reads streaming Delta tables from `spotify_cata.silver.*` into declarative staging tables (`dim_user_stg`, `dim_track_stg`, `dim_date_stg`, `fact_stream_stg`).
* **Slowly Changing Dimensions (SCD Type 2 via Auto CDC):** Implemented `dlt.create_auto_cdc_flow(..., stored_as_scd_type=2)` sequenced by `updated_at` / `date` on dimension tables. Historical changes are automatically tracked with `__START_AT` and `__END_AT` timestamps.
* **Fact Table Upserts (SCD Type 1):** Configured `stored_as_scd_type=1` sequenced by `stream_timestamp` on `FactStream` for idempotent incremental upserts.
* **Data Quality Expectations:** Enforced declarative validation rules using `@dlt.expect_all_or_drop` (e.g., `user_id IS NOT NULL`) to automatically filter malformed records before they reach downstream consumers.

<img width="1165" height="814" alt="image" src="https://github.com/user-attachments/assets/f3d36995-0a67-43fe-8a8c-cd5f9b0f19b2" />

### Gold Pipeline

https://github.com/chaghaas/azureproject/tree/81803a0913c1ff3af9c6cd52954d32f8c18269bb/AzureProject%20-%20databricks/spotify_dab/src/gold/SDP/transformations
---

## ✅ End-to-End Incremental & SCD Type 2 Verification

To validate the entire pipeline under real-world conditions:
1. Executed an incremental SQL script inserting new and updated records into Azure SQL Database.
2. Triggered the ADF `Incremental_Loop` pipeline—confirming only delta records after the latest JSON watermark were extracted.
3. Ran the Silver Auto Loader notebook—verifying checkpointing processed strictly the newly arrived Parquet files.
4. Executed the Gold DLT pipeline—confirming **SCD Type 2** automatically expired modified records (populating `__END_AT`) and inserted the latest active versions (`__END_AT IS NULL`).

<img width="1792" height="568" alt="image" src="https://github.com/user-attachments/assets/ba66c72d-e4d8-4d3d-b2e2-2db6eef935bc" />

---
