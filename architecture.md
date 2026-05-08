# Architecture Diagram (description for `architecture.png`)

Use this description to draw the diagram in **draw.io**, **Excalidraw**, or **Lucidchart**
and export it as `architecture.png` in the project root.

```
                ┌──────────────────────┐
                │   Public REST API    │
                │  (FakeStore / Jobs)  │
                └─────────┬────────────┘
                          │  HTTPS GET /products
                          ▼
                ┌──────────────────────┐
                │  Azure Data Factory  │
                │  - Copy Activity     │
                │  - Notebook Activity │
                └─────────┬────────────┘
                          │  lands JSON
                          ▼
                ┌──────────────────────┐
                │  ADLS Gen2 (RAW)     │   ◄── data/raw/  (locally)
                │  /raw/products/...   │
                └─────────┬────────────┘
                          │
            ┌─────────────┴─────────────┐
            │  Azure Databricks (Spark) │
            │                           │
            │  ┌────────────┐           │
            │  │  BRONZE    │  raw-as-is, schema-on-read
            │  └─────┬──────┘           │
            │        ▼                  │
            │  ┌────────────┐           │
            │  │  SILVER    │  cleaned, deduped, typed
            │  └─────┬──────┘           │
            │        ▼                  │
            │  ┌────────────┐           │
            │  │   GOLD     │  KPIs, business marts
            │  └─────┬──────┘           │
            └────────┼──────────────────┘
                     │  Delta tables (ADLS Gen2)
                     ▼
                ┌──────────────────────┐
                │   Azure SQL DB       │   ◄── SQLite (locally)
                │   (Serving Layer)    │
                └─────────┬────────────┘
                          │
                          ▼
                ┌──────────────────────┐
                │      Power BI        │
                │     Dashboards       │
                └──────────────────────┘
```

## Key design decisions

- **Medallion architecture** (Bronze / Silver / Gold) for clear separation of concerns.
- **Delta Lake** everywhere downstream of raw — gives ACID, MERGE, time travel.
- **ADF** for orchestration so the pipeline is *declarative* and *retryable*.
- **Schema-on-read** at Bronze, **schema-on-write** at Silver and Gold.
- **Idempotent** writes — every notebook can be re-run safely.
