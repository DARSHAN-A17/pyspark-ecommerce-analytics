# Big Data Analytics for E-Commerce — Spark Project

One-notebook case study: **customer behaviour, sales trends and product affinity
on the UCI Online Retail dataset, using local Apache Spark.**

## 1. Problem, background and business impact

Online shops record every transaction (product, quantity, price, customer,
timestamp, country). After a year this is hundreds of thousands of rows —
too clumsy for spreadsheets, ideal for a distributed engine. Raw rows are
also messy (missing customer IDs, duplicates, cancellations, returns,
zero prices) and answer no business question by themselves.

This project builds a repeatable pipeline that cleans the data and turns it
into decisions: which products to stock, which countries to focus on, when
the seasonal peak hits, who the best customers are, and which products to
bundle. Business impact: better stock planning, country-specific marketing,
retention offers for at-risk customers, and cross-sell bundles.

## 2. Objectives and expected outcomes

1. Load the real local retail dataset into Spark.
2. Assess data quality with real counts and clean it with justified steps.
3. Engineer revenue/date features; organise work as Bronze → Silver → Gold.
4. Answer the core business questions with Spark DataFrames + Spark SQL (see §6).
5. Segment customers with RFM, discover data-driven groups with Spark MLlib
   K-Means, find product affinities (support/confidence/lift), and test
   revenue concentration with a Pareto analysis.

## 3. Dataset

- **Source:** UCI Machine Learning Repository, *Online Retail* (UK-based
  online gift retailer). File used here: `Online Retail.xlsx`, placed in the
  project folder (already present; only ever read, never modified).
- **Size:** 541,909 transaction rows + header, 8 columns.
- **Period:** 01 Dec 2010 – 09 Dec 2011.
- **Columns:** `InvoiceNo` (C-prefix = cancellation), `StockCode`, `Description`,
  `Quantity` (negative = return/adjustment), `InvoiceDate`, `UnitPrice`,
  `CustomerID` (missing in ~25% of rows), `Country` (38 countries).

## 4. Pipeline: loading, storage, cleaning, features

- **Loading/storage:** the notebook finds `*.xlsx` automatically (portable
  relative path, clear error if missing), stream-converts it once to
  `online_retail_cache.csv` (regenerable, git-ignored), and reads it into
  Spark with an **explicit schema** (correct types, no guessing).
- **Cleaning (each step shows before → after counts):** snake_case columns;
  null descriptions → `UNKNOWN` (all were zero-price freebies, 0 rows lost);
  5,268 exact duplicates dropped → **536,641-row Silver dataset**;
  `is_cancelled` / `is_return` / `bad_price` flags (missing IDs and returns
  are kept and analysed separately, never silently deleted).
- **Feature engineering:** `revenue = quantity × unit_price` (negative for
  refunds — correct), `line_type` (sale/return), `year`, `month`, `year_month`.
- **Bronze/Silver/Gold:** Bronze = raw rows; Silver = cleaned + flagged +
  featured transactions; Gold = `gold_monthly`, `gold_product`,
  `gold_country` aggregates (+ temp views) feeding the charts and SQL.
- **Spark usage:** DataFrame filter/select/withColumn/groupBy/agg/sort,
  window functions (`row_number` for per-country ranks, `ntile` for RFM
  quintiles), and Spark SQL over temp views (`sales_view`, `rfm`, …).
  Heavy work stays in Spark; only small aggregated tables are collected for
  display/plotting (safe on 8 GB RAM).

## 5. Why Apache Spark

One engine for ETL, SQL and aggregations; in-memory parallel execution
(`local[2]`, 2 GB driver, 8 shuffle partitions runs this dataset in seconds
per step on a laptop); the same code scales to a cluster with no rewrite.

## 6. Methods, KPIs and actual findings (executed outputs)

Valid-sales view = positive quantity, positive price, not cancelled
(524,878 lines). Headline results:

| # | Question | Result |
|---|----------|--------|
| 1 | Top products by quantity | `23843` PAPER CRAFT LITTLE BIRDIE (80,995 units), `23166`, `84077`, … |
| 2 | Top products by revenue | Revenue leaders differ from quantity leaders (price matters as much as volume) |
| 3 | Top countries by revenue | UK dominates (>90% of rows); then Germany, France, EIRE, Netherlands |
| 4 | Top 5 products × top 5 countries | 25-row ranked table (rank, qty, revenue) — e.g. France #1 `23084` RABBIT NIGHT LIGHT (4,024 units); Germany #3 is `POSTAGE` (a service-charge line, not a product) |
| 5 | Monthly trend + peak | Autumn 2011 peak (Christmas stocking); **Dec 2011 is partial — data ends 9 Dec 2011** |
| 6 | Products bought together | Top pair `22386 + 85099B` in 825 invoices (support 0.041, lift 6.47); limited to top-30 products (laptop constraint) |
| 7 | Input → output trace | Invoice `536365` followed raw → cleaned → SQL result |
| 8 | K-Means customer groups (MLlib, §12) | k=4, silhouette 0.55 on 4,338 customers (scaled recency/log-F/log-M): VIP heavy (568, ~£9.9k avg), active core (1,449, ~£1.7k), recent light (1,382, ~£386), dormant/light (939, ~£386, ~259 days idle); k=3 scores marginally higher (0.60), neither claimed optimal |
| 9 | Revenue concentration (Pareto, §13) | 837 of 4,161 products (20.12%) make the first 80% of ~£10.64M revenue (top 10% → 63.5%, top 20% → 79.9%); merchandise-only rerun agrees (859 of 4,156, 20.67%); #1 line is postage (`DOT`), handled by an explicit service-code scope |

KPIs: revenue ≈ **£10.64M**, **19,960** valid invoices, **AOV ≈ £533.17**,
4,338 RFM customers (1,224 Champions, 510 Loyal, 209 At Risk, 1,117
Lost/Dormant). Charts: monthly revenue, top-10 products, top-10 countries,
RFM segments, top-8 affinity pairs, K-Means cluster sizes + recency-vs-spend
scatter, Pareto top-25 bars + cumulative line (8 charts total).

## 7. Limitations (honest reading of historical data)

- ~25% of rows have no CustomerID → customer-level analysis covers identified buyers only.
- Cancellations are flagged, not matched back to original sales.
- Affinity covers the top-30 products only; lift is descriptive, not causal.
- One retailer, one year (ending 9 Dec 2011) — patterns may not generalise.
- Tested stack is Python 3.14 + PySpark 3.5.5, which forbids `toPandas()`,
  UDFs and `createDataFrame` from Python objects (fixed by design, see notebook).
  `pyspark.ml` additionally needs a notebook-local `distutils` stub on
  Python 3.12+ (CPython removed `distutils`; PySpark 3.5.5 still imports it) —
  the notebook installs the stub itself and MLlib K-Means runs tested-OK.
- K-Means covers ID-carrying customers only, prefers round clusters, and
  k=4 is justified by balance + interpretability (k=3 scores marginally higher).
- Pareto covers one retailer-year (December 2011 partial); revenue rank mixes
  price and demand; postage/carriage lines are audited and re-run separately.

## 8. Tested environment

| Component | Tested (this laptop) | Recommended for another laptop |
|-----------|----------------------|-------------------------------|
| OS | Windows 11 | Windows 10/11 (Linux/macOS also fine) |
| Python | 3.14.6 | **3.10 or 3.11** (smoothest with Spark 3.5) |
| Java | Temurin 17.0.20 | Java 11 or 17 (64-bit) |
| PySpark | 3.5.5 | 3.5.x |
| pandas / matplotlib / openpyxl / notebook | 3.0.5 / 3.11.2 / 3.1.5 / 7.6.3 | see `requirements.txt` |
| Hardware | 8 GB RAM, CPU-only, local Spark | 8 GB+ RAM, CPU-only is fine |

## 9. Reproduce on another laptop

1. Install Python 3.10+ (3.11 ideal), Java 11/17 (64-bit).
2. Copy this folder's files **plus** `Online Retail.xlsx` into one folder.
3. Double-click / run `setup_windows.bat` (creates `.venv`, installs
   `requirements.txt`, runs the environment check).
   Manual alternative: `pip install -r requirements.txt`, then
   `python check_environment.py`.
4. Launch: `jupyter notebook BDA_Complete_Analytics.ipynb` → Run All.
   First run converts the Excel file to a cache CSV (~2–3 min); later runs reuse it.
5. If the dataset is missing, the notebook stops immediately with a clear
   `FileNotFoundError` telling you where to put the file.

## 10. Files

| File | Role |
|------|------|
| `BDA_Complete_Analytics.ipynb` | The complete project (code + results + charts + conclusion) |
| `Online Retail.xlsx` | Original dataset (read-only input, keep it) |
| `online_retail_cache.csv` | Regenerable conversion cache (auto-rebuilt, git-ignored) |
| `requirements.txt` | Pinned dependencies |
| `setup_windows.bat` | One-step Windows setup |
| `check_environment.py` | Pre-flight check (exit 0 = ready) |
| `.gitignore` | Ignores `.venv/`, cache, checkpoints |
| `README.md` | This case-study + setup document |

## 11. Test status

- Notebook executed end-to-end in this environment: 82 cells, 0 errors,
  8 charts, real outputs (verified 2026-10-11; rerun writes fresh outputs
  into `BDA_Complete_Analytics.ipynb`).
- `check_environment.py`: tested here (must report READY).
- `setup_windows.bat`: installer flow tested in an isolated folder (fresh venv +
  pip install + env check); a full notebook re-run inside that fresh venv was
  **not** repeated (same machine, same files — see report in chat).
