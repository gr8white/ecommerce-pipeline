# Olist E-Commerce Pipeline

A production-style data engineering pipeline built on Databricks Free Edition. The project simulates a real client engagement: Olist, a Brazilian e-commerce marketplace, needs a reliable pipeline to turn raw transactional CSVs into governed, queryable analytics tables — with CI/CD deployment and access controls in place from day one.

This project is intentionally built end-to-end: raw file ingestion → bronze → silver → gold, deployed via a Declarative Automation Bundle, governed with Unity Catalog, and shipped to production through GitHub Actions.

---

## Dataset

[Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — a public dataset of ~100k orders from 2016–2018 across multiple Brazilian marketplaces. It includes orders, customers, sellers, products, payments, reviews, and geolocation data across 9 CSV files.

---

## Tools and Technologies

| Tool | Purpose |
|---|---|
| Databricks Free Edition | Workspace, compute, Unity Catalog, SQL Warehouses |
| Unity Catalog | Three-level namespace (`catalog.schema.table`), access governance |
| PySpark | Bronze ingestion, Silver transformations |
| Lakeflow Declarative Pipelines (LDP) | Gold materialized views and streaming tables via `@dlt.table` |
| Lakeflow Jobs | DAG-based orchestration with file arrival trigger |
| Declarative Automation Bundles (DABs) | Infrastructure-as-code via `databricks.yml` |
| Databricks CLI v1.4+ | Local bundle validation and deployment |
| GitHub Actions | CI/CD — auto-deploy to prod on merge to `main` |

---

## Topics Covered

- **Medallion Architecture** — Bronze (raw), Silver (cleaned), Gold (aggregated) layer design
- **PySpark Transformations** — type casting, deduplication via window functions, `to_timestamp`, `row_number`
- **Inline Data Quality** — `assert_true()` for null checks and enum validation directly in the transform chain
- **Lakeflow Declarative Pipelines** — `@dlt.table` for materialized views and streaming tables, `spark.readStream` for live data
- **Lakeflow Jobs** — multi-task DAGs, task dependencies, `run_if: ALL_DONE`, file arrival triggers, retry configuration
- **Unity Catalog Governance** — `GRANT`/`REVOKE`, column masking with `is_account_group_member()`, row-level security filters
- **Declarative Automation Bundles** — `databricks.yml`, modular `resources/*.yml`, `${workspace.file_path}` variable resolution, dev/prod targets
- **CI/CD with GitHub Actions** — automated `databricks bundle deploy --target prod` on push to `main`

---

## Project Structure

```
ecommerce-pipeline/
├── databricks.yml                  # Bundle root config (targets, variables)
├── resources/
│   ├── bronze_job.yml              # Main pipeline job (file arrival trigger, full DAG)
│   ├── silver_job.yml              # Silver-only job (reserved)
│   └── gold_job.yml                # Scheduled gold metrics job
├── src/
│   ├── bronze/
│   │   ├── ingest_raw.py           # Reads all 9 CSVs from Volume → bronze Delta tables
│   │   └── validate_counts.py      # Row count assertions before silver runs
│   ├── silver/
│   │   ├── silver_customers.py
│   │   ├── silver_orders.py        # Type casting, deduplication, DQ checks
│   │   ├── silver_order_items.py
│   │   ├── silver_products.py
│   │   └── silver_sellers.py
│   ├── gold/
│   │   ├── build_metrics.ipynb     # Notebook task — runs on SQL warehouse
│   │   ├── gold_declarative.py     # LDP pipeline: monthly_revenue (MV) + live_orders (ST)
│   │   └── notification.py        # Final task — prints pipeline completion status
│   └── governance.ipynb            # Unity Catalog: grants, column masks, row filters
└── .github/
    └── workflows/
        └── deploy.yml              # GitHub Actions: deploy to prod on merge to main
```

---

## How to Rebuild This Project

### Prerequisites

- [Databricks Free Edition account](https://www.databricks.com/try-databricks)
- [Databricks CLI v1.4+](https://docs.databricks.com/dev-tools/cli/install.html)
- [Git](https://git-scm.com/) and a GitHub account
- Python 3.8+

---

### Step 1 — Set Up Your Databricks Workspace

1. Sign in to your Databricks Free Edition workspace.
2. Go to **Catalog** and create a new catalog named `ecommerce`.
3. Inside `ecommerce`, create three schemas: `bronze`, `silver`, `gold`.
4. Go to **Catalog → Volumes** and create an external volume at `ecommerce.bronze.raw_files`.
5. Download the [Olist dataset from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and upload all 9 CSV files to the volume.

---

### Step 2 — Configure the Databricks CLI

```bash
databricks configure
# Enter your workspace host (e.g. https://dbc-xxxxxxxx.cloud.databricks.com)
# Enter your personal access token (generate one in Settings → Developer → Access Tokens)
```

Verify it works:

```bash
# Run from your home directory (~), not inside the repo
cd ~
databricks workspace list /
```

> **Note:** Always run non-bundle CLI commands from outside the repo directory. Running from inside the repo causes the CLI to pick up `databricks.yml` and require a `--target` flag.

---

### Step 3 — Clone the Repo and Configure the Bundle

```bash
git clone <your-repo-url>
cd ecommerce-pipeline
```

Open `databricks.yml` and update the workspace host and your email in the `prod` root path:

```yaml
targets:
  dev:
    default: true
    mode: development
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
  prod:
    mode: production
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
      root_path: /Workspace/Users/<your-email>/.bundle/${bundle.name}/${bundle.target}
```

Open `resources/bronze_job.yml` and `resources/gold_job.yml`. Find the `warehouse_id` fields and replace them with your actual SQL warehouse ID:

```bash
# Find your warehouse ID:
# SQL Warehouses → click your warehouse → Connection details → HTTP path
# The ID is the last segment: /sql/1.0/warehouses/<THIS_PART>
```

---

### Step 4 — Create the Lakeflow Declarative Pipeline

The `gold_declarative.py` file must be attached to a Lakeflow (DLT) pipeline manually — it cannot be deployed via DAB on Free Edition.

1. In your workspace, go to **Delta Live Tables → Create Pipeline**.
2. Name it `gold_metrics_pipeline`.
3. Set the source to the path where `gold_declarative.py` lives in your workspace (after syncing, it will be at `/Workspace/Users/<your-email>/...`).
4. Set the target catalog to `ecommerce` and target schema to `gold`.
5. Run the pipeline once to create the `monthly_revenue` (materialized view) and `live_orders` (streaming table).

---

### Step 5 — Deploy the Bundle

**Test locally against dev first:**

```bash
# From inside the repo
databricks bundle validate
databricks bundle deploy --target dev
```

**Deploy to prod:**

```bash
databricks bundle deploy --target prod
```

This creates the jobs in your Databricks workspace under **Workflows**. The main job (`[prod] Ecommerce Pipeline`) is configured with a file arrival trigger — it will run automatically whenever new files land in `/Volumes/ecommerce/bronze/raw_files/`.

---

### Step 6 — Set Up CI/CD with GitHub Actions

1. In your GitHub repo, go to **Settings → Secrets and Variables → Actions**.
2. Add two secrets:
   - `DATABRICKS_HOST` — your workspace URL
   - `DATABRICKS_TOKEN` — your personal access token
3. Any push to `main` will now automatically run `databricks bundle deploy --target prod`.

---

### Step 7 — Set Up Governance (Optional)

Open `src/governance.ipynb` in your Databricks workspace and run through the cells to:

- Create `ecommerce_analysts` and `ecommerce_admins` groups (do this first in **Settings → Identity and Access → Groups**)
- Grant read access to silver/gold tables for the analyst group
- Apply a column mask on `customer_zip_code_prefix` in `silver.customers`
- Apply a row-level filter on `silver.sellers` restricting by state

---

## Running the Pipeline Manually

Once deployed, go to **Workflows** in your workspace, find `[prod] Ecommerce Pipeline`, and click **Run now**. The DAG executes in this order:

```
bronze_ingestion
    └── data_quality_check
            ├── silver_customers
            ├── silver_orders
            ├── silver_order_items
            ├── silver_products
            └── silver_sellers
                    └── gold_build
                            └── notification (ALL_DONE)
```

---

## Free Edition Limitations

A few things work differently on Databricks Free Edition vs. paid tiers:

- **Materialized views and streaming tables** must be created through a Lakeflow (DLT) pipeline — plain SQL `CREATE MATERIALIZED VIEW` is not supported on serverless generic compute.
- **`dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()`** is blocked by the security manager. Use `context.apiUrl()` or `context.toJson()` instead.
- **User impersonation** for testing governance rules isn't available. Test by adding/removing your own account from groups and refreshing your session.
- **Notebook tasks with a `warehouse_id`** run on SQL warehouse compute and cannot use `environment_key` (that's for serverless Python tasks only).
