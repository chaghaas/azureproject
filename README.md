End-to-End Azure Data Engineering Project

## 📌 Project Overview

An end-to-end, production-style **Azure Data Engineering** pipeline built on a Spotify streaming dataset. The project implements the **Medallion Architecture (Bronze → Silver → Gold)** to ingest, cleanse, govern, and model streaming and user data into an analytics-ready **Star Schema**.

Key engineering patterns demonstrated in this repository include **dynamic metadata-driven ingestion**, **JSON-based CDC watermarking**, **backdated refresh (backfilling)**, **Spark Structured Streaming with Auto Loader**, **Jinja2-powered dynamic SQL generation**, **Unity Catalog governance**, and **Slowly Changing Dimensions (SCD Type 2)** using **Lakeflow Declarative Pipelines (Delta Live Tables)**.
