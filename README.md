# Azure + Databricks End-to-End Data Engineering Project

> A production-style data engineering portfolio project that ingests data from a public API,
> lands it on Azure Data Lake Gen2 (simulated locally), transforms it through the
> **Bronze → Silver → Gold (Medallion Architecture)** using PySpark + Delta Lake,
> and exposes business-ready tables to Azure SQL / SQLite for downstream BI tools like Power BI.

---

## 1. Project Overview

This project demonstrates a complete, real-world **Azure Data Engineer** workflow:

```
Public API  →  Azure Data Factory  →  ADLS Gen2  →  Azure Databricks (PySpark + Delta)  →  Azure SQL  →  Power BI
              (orchestration)        (raw zone)       (Bronze / Silver / Gold)               (serving)     (BI)
```

The pipeline ingests **e-commerce product data** from the public
[FakeStore API](https://fakestoreapi.com/products), stores it in raw JSON,
and then incrementally refines it into analytics-ready tables.

Although it is designed for **Azure Data Factory + Azure Databricks + Azure SQL + ADLS Gen2**,
everything has been written so it can run **100% locally** with no Azure subscription required —
making it perfect for portfolios, GitHub, demos, and interview prep.

---

## 2. Architecture

![Architecture](architecture.png)

> The image is described in detail in `architecture.md`. If you want a real PNG,
> render the description in [draw.io](https://app.diagrams.net) or [Excalidraw](https://excalidraw.com).

### High-level flow

1. **Ingestion** — A Python job (or Azure Data Factory `Copy Activity`) calls the
   public FakeStore API and lands the raw JSON into the `raw/` zone of the data lake.
2. **Bronze Layer** — PySpark reads the raw JSON as-is and writes it to a Delta table.
   This is the "single source of truth" of what the source returned.
3. **Silver Layer** — PySpark cleans the data (removes nulls, fixes types, normalizes
   nested fields, deduplicates) and writes a curated Delta table.
4. **Gold Layer** — PySpark aggregates the Silver data into business-level KPIs
   (revenue per category, top-rated products, etc.) and writes Delta tables ready for BI.
5. **Serving** — The Gold layer is exported to **Azure SQL Database** (SQLite locally)
   and consumed by **Power BI**.

---

## 3. Tech Stack

| Layer            | Tool (Cloud)                     | Local Substitute                |
|------------------|----------------------------------|---------------------------------|
| Orchestration    | Azure Data Factory               | Plain Python script             |
| Storage (raw)    | Azure Data Lake Storage Gen2     | `data/raw/` folder              |
| Compute          | Azure Databricks (PySpark)       | Local PySpark / Databricks CE   |
| Storage (curated)| Delta Lake on ADLS Gen2          | `data/bronze|silver|gold/`      |
| Serving DB       | Azure SQL Database               | SQLite                          |
| BI               | Power BI                         | (conceptual — see README)       |
| Language         | Python 3.10+, PySpark 3.5, SQL   |                                 |

---

## 4. Repository Structure

```
project/
│
├── README.md
├── architecture.md            # Detailed description of architecture.png
├── architecture.png           # Architecture diagram (replace with your own export)
├── requirements.txt
│
├── data/
│   ├── raw/                   # JSON dropped here by the ingestion job
│   ├── bronze/                # Delta tables — raw-as-is
│   ├── silver/                # Delta tables — cleaned & conformed
│   └── gold/                  # Delta tables — business aggregates
│
├── ingestion/
│   └── ingest_api.py          # Pulls FakeStore API → data/raw/
│
├── databricks/
│   ├── bronze_layer.py        # raw JSON   → Delta (Bronze)
│   ├── silver_layer.py        # Bronze     → Delta (Silver, cleaned)
│   └── gold_layer.py          # Silver     → Delta (Gold, aggregates)
│
├── sql/
│   ├── schema.sql             # DDL for serving layer (Azure SQL / SQLite)
│   └── queries.sql            # Analytical queries on the Gold tables
│
└── pipelines/
    └── adf_pipeline.json      # Azure Data Factory pipeline definition (Copy + Notebook)
```

---

## 5. Step-by-Step Execution (Local)

### 5.1 Prerequisites

- Python 3.10 or higher
- Java 8 or 11 (required by PySpark)
- ~2 GB free disk space

### 5.2 Setup

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/azure-databricks-portfolio.git
cd azure-databricks-portfolio/project

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate          # macOS/Linux
# .venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

### 5.3 Run the Pipeline

```bash
# Step 1 — Ingest raw JSON from the public API
python ingestion/ingest_api.py

# Step 2 — Bronze layer (raw → Delta)
python databricks/bronze_layer.py

# Step 3 — Silver layer (clean & conform)
python databricks/silver_layer.py

# Step 4 — Gold layer (business aggregates)
python databricks/gold_layer.py

# Step 5 — Load Gold into the serving database (SQLite for demo)
sqlite3 data/serving.db < sql/schema.sql
python -c "from databricks.gold_layer import export_gold_to_sqlite; export_gold_to_sqlite()"

# Step 6 — Run analytical queries
sqlite3 data/serving.db < sql/queries.sql
```

### 5.4 Run on Real Azure (optional)

1. Create an **Azure Data Lake Gen2** account and four containers: `raw`, `bronze`, `silver`, `gold`.
2. Import `pipelines/adf_pipeline.json` into **Azure Data Factory**
   (Author → Pipelines → New from JSON).
3. Upload the three notebooks under `databricks/` to your **Azure Databricks** workspace.
4. Replace the local paths (`./data/...`) with `abfss://<container>@<storage>.dfs.core.windows.net/...`.
5. Trigger the ADF pipeline — it will copy from the API, then run the three notebooks.

---

## 6. Screenshots (placeholders)

Add real screenshots in `docs/screenshots/` and link them here:

- `docs/screenshots/01_adf_pipeline.png` — Azure Data Factory pipeline run
- `docs/screenshots/02_databricks_bronze.png` — Bronze notebook output
- `docs/screenshots/03_databricks_silver.png` — Silver notebook with `display(df)`
- `docs/screenshots/04_gold_table.png` — Gold layer Delta table preview
- `docs/screenshots/05_powerbi_dashboard.png` — Power BI dashboard
- `docs/screenshots/06_sql_queries.png` — Analytical SQL output

---

## 7. Power BI (Conceptual)

Once the Gold layer is loaded into Azure SQL, connect Power BI Desktop:

1. **Get Data → Azure SQL Database** → enter server / DB / credentials.
2. Import the three Gold tables: `gold_category_kpis`, `gold_top_products`, `gold_price_buckets`.
3. Build visuals such as:
   - **Bar chart**: Revenue by category
   - **Card**: Total products / Average rating
   - **Table**: Top 10 highest-rated products
   - **Slicer**: Category filter
4. Publish to the Power BI Service and schedule a daily refresh that aligns with the ADF pipeline.

---

## 8. Resume Bullet Points

Copy / adapt these directly into your CV:

- Designed and implemented an end-to-end **Azure data pipeline** (ADF → ADLS Gen2 → Databricks → Azure SQL → Power BI) following the **Medallion Architecture (Bronze / Silver / Gold)**.
- Built ingestion in **Python** that pulls from REST APIs and lands raw JSON to **Azure Data Lake Storage Gen2**, orchestrated by **Azure Data Factory**.
- Developed **PySpark** notebooks on **Azure Databricks** to clean, normalize, and aggregate data into **Delta Lake** tables, achieving ACID guarantees and time travel.
- Engineered **business-ready Gold tables** (revenue by category, top-rated products, price buckets) and exposed them to **Azure SQL** for **Power BI** dashboards.
- Authored **DDL and analytical SQL** for the serving layer and parameterized **ADF pipeline JSON** for repeatable deployments.
- Ensured the codebase is **modular, idempotent, and locally runnable**, enabling fast iteration without cloud costs.

---

## 9. License

MIT — free to fork, learn from, and adapt for your own portfolio.
