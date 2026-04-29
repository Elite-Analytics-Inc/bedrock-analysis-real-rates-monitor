import os, sys
sys.path.insert(0, "/")
from bedrock_sdk import BedrockJob

job = BedrockJob()
conn = job.connect()

start_date = os.environ.get("PARAM_START_DATE", "2010-01-01")

job.update_progress("running_analysis", progress_pct=5, progress_message="Loading FRED macro data…")

# ── 1. Load raw series and pivot to wide format ─────────────────────────────
job.fetch("fred_raw", f"""
    SELECT date, series_id, value
    FROM bedrock.finance.fred_macro
    WHERE series_id IN ('DGS10','CPIAUCSL','FEDFUNDS','MORTGAGE30US','MMNRNJ')
      AND date >= DATE '{start_date}'
    ORDER BY date
""")

job.update_progress("running_analysis", progress_pct=20, progress_message="Pivoting series to wide format…")

# Pivot: one row per date, one column per series
conn.execute("""
    CREATE OR REPLACE TABLE wide AS
    SELECT
        date,
        MAX(CASE WHEN series_id = 'DGS10'        THEN value END) AS dgs10,
        MAX(CASE WHEN series_id = 'CPIAUCSL'     THEN value END) AS cpiaucsl,
        MAX(CASE WHEN series_id = 'FEDFUNDS'     THEN value END) AS fedfunds,
        MAX(CASE WHEN series_id = 'MORTGAGE30US' THEN value END) AS mortgage30us,
        MAX(CASE WHEN series_id = 'MMNRNJ'       THEN value END) AS mmnrnj
    FROM fred_raw
    GROUP BY date
    ORDER BY date
""")

job.update_progress("running_analysis", progress_pct=35, progress_message="Computing YoY inflation and real rates…")

# ── 2. Compute YoY CPI inflation via 12-month lag, then real rates ───────────
conn.execute("""
    CREATE OR REPLACE TABLE metrics AS
    WITH lagged AS (
        SELECT
            date,
            dgs10,
            cpiaucsl,
            fedfunds,
            mortgage30us,
            mmnrnj,
            LAG(cpiaucsl, 12) OVER (ORDER BY date) AS cpi_lag_12
        FROM wide
    )
    SELECT
        date,
        dgs10,
        cpiaucsl,
        fedfunds,
        mortgage30us,
        mmnrnj,
        cpi_lag_12,
        -- YoY inflation
        CASE
            WHEN cpi_lag_12 IS NOT NULL AND cpi_lag_12 > 0
            THEN 100.0 * (cpiaucsl / cpi_lag_12 - 1.0)
            ELSE NULL
        END AS inflation_yoy,
        -- Fisher Gap: real 10Y rate
        CASE
            WHEN cpi_lag_12 IS NOT NULL AND cpi_lag_12 > 0
            THEN dgs10 - (100.0 * (cpiaucsl / cpi_lag_12 - 1.0))
            ELSE NULL
        END AS real_rate_10y,
        -- Real mortgage rate
        CASE
            WHEN cpi_lag_12 IS NOT NULL AND cpi_lag_12 > 0
            THEN mortgage30us - (100.0 * (cpiaucsl / cpi_lag_12 - 1.0))
            ELSE NULL
        END AS real_mortgage_30y,
        -- Real deposit yield (money market)
        CASE
            WHEN cpi_lag_12 IS NOT NULL AND cpi_lag_12 > 0 AND mmnrnj IS NOT NULL
            THEN mmnrnj - (100.0 * (cpiaucsl / cpi_lag_12 - 1.0))
            ELSE NULL
        END AS real_deposit_yield
    FROM lagged
    WHERE cpi_lag_12 IS NOT NULL
    ORDER BY date
""")

job.update_progress("running_analysis", progress_pct=55, progress_message="Writing fisher_gap parquet…")

# ── 3. OUTPUT 1: fisher_gap.parquet ─────────────────────────────────────────
job.write_parquet("fisher_gap", """
    SELECT
        date,
        dgs10,
        inflation_yoy,
        real_rate_10y
    FROM metrics
    WHERE dgs10 IS NOT NULL
      AND inflation_yoy IS NOT NULL
    ORDER BY date
""")

job.update_progress("running_analysis", progress_pct=65, progress_message="Writing deposit_vs_loan parquet…")

# ── 4. OUTPUT 2: deposit_vs_loan.parquet ────────────────────────────────────
conn.execute("""
    CREATE OR REPLACE TABLE deposit_vs_loan AS
    WITH latest AS (
        SELECT MAX(date) AS max_date FROM metrics
    )
    SELECT
        'mortgage_30y'    AS product,
        mortgage30us      AS nominal_yield,
        real_mortgage_30y AS real_yield
    FROM metrics, latest WHERE date = latest.max_date
      AND mortgage30us IS NOT NULL
    UNION ALL
    SELECT
        'deposit_savings' AS product,
        mmnrnj            AS nominal_yield,
        real_deposit_yield AS real_yield
    FROM metrics, latest WHERE date = latest.max_date
      AND mmnrnj IS NOT NULL
""")

job.write_parquet("deposit_vs_loan", "SELECT * FROM deposit_vs_loan")

job.update_progress("running_analysis", progress_pct=72, progress_message="Writing trend parquet…")

# ── 5. OUTPUT 3: trend.parquet ───────────────────────────────────────────────
conn.execute("""
    CREATE OR REPLACE TABLE trend AS
    WITH ordered AS (
        SELECT date, real_rate_10y,
               ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
        FROM metrics
        WHERE real_rate_10y IS NOT NULL
    )
    SELECT
        AVG(CASE WHEN rn <= 6  THEN real_rate_10y END) AS recent_6m_avg,
        AVG(CASE WHEN rn > 6 AND rn <= 12 THEN real_rate_10y END) AS prior_6m_avg,
        AVG(CASE WHEN rn <= 6  THEN real_rate_10y END) -
        AVG(CASE WHEN rn > 6 AND rn <= 12 THEN real_rate_10y END) AS trend_delta,
        CASE
            WHEN AVG(CASE WHEN rn <= 6  THEN real_rate_10y END) >
                 AVG(CASE WHEN rn > 6 AND rn <= 12 THEN real_rate_10y END)
            THEN 'up' ELSE 'down'
        END AS trend_direction
    FROM ordered
""")

job.write_parquet("trend", "SELECT * FROM trend")

job.update_progress("running_analysis", progress_pct=82, progress_message="Writing KPI parquet…")

# ── 6. OUTPUT 4: kpis.parquet ────────────────────────────────────────────────
conn.execute("""
    CREATE OR REPLACE TABLE kpis AS
    WITH latest_row AS (
        SELECT *
        FROM metrics
        WHERE real_rate_10y IS NOT NULL
          AND dgs10 IS NOT NULL
          AND inflation_yoy IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
    ),
    dvl AS (
        SELECT
            MAX(CASE WHEN product = 'mortgage_30y'    THEN nominal_yield END) AS mortgage_nominal,
            MAX(CASE WHEN product = 'deposit_savings' THEN nominal_yield END) AS deposit_nominal
        FROM deposit_vs_loan
    ),
    tr AS (SELECT trend_delta FROM trend)
    SELECT
        lr.real_rate_10y            AS current_real_rate_10y,
        lr.inflation_yoy            AS current_inflation_yoy,
        lr.dgs10                    AS current_dgs10,
        dvl.mortgage_nominal - dvl.deposit_nominal AS deposit_loan_gap_pct,
        lr.date                     AS last_observation_date
    FROM latest_row lr, dvl, tr
""")

job.write_parquet("kpis", "SELECT * FROM kpis")

job.update_progress("running_analysis", progress_pct=92, progress_message="Logging summary KPIs…")

# ── 7. Log summary table to run log ─────────────────────────────────────────
kpi_rows = conn.execute("SELECT * FROM kpis").fetchall()
if kpi_rows:
    r = kpi_rows[0]
    job.table(
        "kpi_summary",
        "Purchasing Power Monitor — Current KPIs",
        ["Last Obs Date", "10Y Nominal (%)", "Inflation YoY (%)", "Real Rate 10Y (%)", "Deposit-Loan Gap (pp)"],
        [[str(r[4]), f"{r[2]:.2f}", f"{r[1]:.2f}", f"{r[0]:.2f}", f"{r[3]:.2f}" if r[3] is not None else "N/A"]]
    )

job.update_progress("running_analysis", progress_pct=95, progress_message="Finalising…",
    lineage={
        "inputs": ["bedrock.finance.fred_macro"],
        "outputs": [
            "analytics/bedrock/JOB_ID/data/kpis.parquet",
            "analytics/bedrock/JOB_ID/data/fisher_gap.parquet",
            "analytics/bedrock/JOB_ID/data/deposit_vs_loan.parquet",
            "analytics/bedrock/JOB_ID/data/trend.parquet"
        ]
    }
)

job.complete()
