import os, sys
sys.path.insert(0, "/")
from bedrock_sdk import BedrockJob

job = BedrockJob()
conn = job.connect()

start_date = os.environ.get("PARAM_START_DATE", "2010-01-01")

job.update_progress("running_analysis", progress_pct=5, progress_message="Loading FRED macro data…")

# ── 1. Load raw long-format series ──────────────────────────────────────────
job.fetch("fred_raw", f"""
    SELECT date, series_id, value
    FROM bedrock.finance.fred_macro
    WHERE series_id IN ('DGS10','CPIAUCSL','FEDFUNDS','MORTGAGE30US','MMNRNJ')
      AND date >= DATE '{start_date}'
""")

job.update_progress("running_analysis", progress_pct=20,
                    progress_message="Building monthly aligned panel…")

# Each FRED series has a different observation cadence (DGS10 daily, CPI
# monthly, MORTGAGE30US weekly, MMNRNJ weekly until 2021). We need a single
# monthly panel where every column is forward-filled to the month-end.
#
# Strategy:
#   1. Build a master month spine from start_date to MAX(date).
#   2. For each series, take the LAST value within each month.
#   3. Forward-fill across months so MMNRNJ (ends 2021) stays present and
#      DGS10/MORTGAGE30US always have a value.
conn.execute("""
    CREATE OR REPLACE TABLE month_spine AS
    SELECT
        CAST(STRFTIME(generate_series, '%Y-%m-01') AS DATE) AS month
    FROM generate_series(
        DATE '2010-01-01',
        (SELECT MAX(date) FROM fred_raw),
        INTERVAL 1 MONTH
    )
""")

conn.execute("""
    CREATE OR REPLACE TABLE monthly_last AS
    SELECT
        DATE_TRUNC('month', date)::DATE AS month,
        series_id,
        LAST_VALUE(value) OVER (
            PARTITION BY series_id, DATE_TRUNC('month', date)
            ORDER BY date
            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS month_value
    FROM fred_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY series_id, DATE_TRUNC('month', date) ORDER BY date DESC
    ) = 1
""")

conn.execute("""
    CREATE OR REPLACE TABLE wide_raw AS
    SELECT
        s.month,
        MAX(CASE WHEN m.series_id = 'DGS10'        THEN m.month_value END) AS dgs10,
        MAX(CASE WHEN m.series_id = 'CPIAUCSL'     THEN m.month_value END) AS cpiaucsl,
        MAX(CASE WHEN m.series_id = 'FEDFUNDS'     THEN m.month_value END) AS fedfunds,
        MAX(CASE WHEN m.series_id = 'MORTGAGE30US' THEN m.month_value END) AS mortgage30us,
        MAX(CASE WHEN m.series_id = 'MMNRNJ'       THEN m.month_value END) AS mmnrnj
    FROM month_spine s
    LEFT JOIN monthly_last m ON m.month = s.month
    GROUP BY s.month
""")

# Forward-fill via correlated subquery (DuckDB has no native ffill yet).
conn.execute("""
    CREATE OR REPLACE TABLE wide AS
    WITH ffilled AS (
        SELECT
            month,
            COALESCE(dgs10,        (SELECT dgs10        FROM wide_raw w2 WHERE w2.month <= w.month AND w2.dgs10        IS NOT NULL ORDER BY month DESC LIMIT 1)) AS dgs10,
            COALESCE(cpiaucsl,     (SELECT cpiaucsl     FROM wide_raw w2 WHERE w2.month <= w.month AND w2.cpiaucsl     IS NOT NULL ORDER BY month DESC LIMIT 1)) AS cpiaucsl,
            COALESCE(fedfunds,     (SELECT fedfunds     FROM wide_raw w2 WHERE w2.month <= w.month AND w2.fedfunds     IS NOT NULL ORDER BY month DESC LIMIT 1)) AS fedfunds,
            COALESCE(mortgage30us, (SELECT mortgage30us FROM wide_raw w2 WHERE w2.month <= w.month AND w2.mortgage30us IS NOT NULL ORDER BY month DESC LIMIT 1)) AS mortgage30us,
            COALESCE(mmnrnj,       (SELECT mmnrnj       FROM wide_raw w2 WHERE w2.month <= w.month AND w2.mmnrnj       IS NOT NULL ORDER BY month DESC LIMIT 1)) AS mmnrnj
        FROM wide_raw w
    )
    SELECT month AS date, * EXCLUDE (month) FROM ffilled ORDER BY date
""")

job.update_progress("running_analysis", progress_pct=45,
                    progress_message="Computing YoY inflation + real rates…")

conn.execute("""
    CREATE OR REPLACE TABLE metrics AS
    WITH lagged AS (
        SELECT
            *,
            LAG(cpiaucsl, 12) OVER (ORDER BY date) AS cpi_lag_12
        FROM wide
    )
    SELECT
        date,
        dgs10, cpiaucsl, fedfunds, mortgage30us, mmnrnj,
        cpi_lag_12,
        100.0 * (cpiaucsl / cpi_lag_12 - 1.0)                AS inflation_yoy,
        dgs10        - 100.0 * (cpiaucsl / cpi_lag_12 - 1.0) AS real_rate_10y,
        mortgage30us - 100.0 * (cpiaucsl / cpi_lag_12 - 1.0) AS real_mortgage_30y,
        mmnrnj       - 100.0 * (cpiaucsl / cpi_lag_12 - 1.0) AS real_deposit_yield
    FROM lagged
    WHERE cpi_lag_12 IS NOT NULL
      AND cpi_lag_12 > 0
      AND dgs10 IS NOT NULL
    ORDER BY date
""")

job.update_progress("running_analysis", progress_pct=60,
                    progress_message="Writing fisher_gap parquet…")

job.write_parquet("fisher_gap", """
    SELECT date, dgs10, inflation_yoy, real_rate_10y
    FROM metrics
    ORDER BY date
""")

job.update_progress("running_analysis", progress_pct=70,
                    progress_message="Writing deposit_vs_loan parquet…")

# Latest non-null observation for each product
conn.execute("""
    CREATE OR REPLACE TABLE deposit_vs_loan AS
    WITH latest_mort AS (
        SELECT mortgage30us, real_mortgage_30y
        FROM metrics
        WHERE mortgage30us IS NOT NULL AND real_mortgage_30y IS NOT NULL
        ORDER BY date DESC LIMIT 1
    ),
    latest_dep AS (
        SELECT mmnrnj, real_deposit_yield
        FROM metrics
        WHERE mmnrnj IS NOT NULL AND real_deposit_yield IS NOT NULL
        ORDER BY date DESC LIMIT 1
    )
    SELECT 'mortgage_30y'    AS product, lm.mortgage30us AS nominal_yield, lm.real_mortgage_30y AS real_yield FROM latest_mort lm
    UNION ALL
    SELECT 'deposit_savings' AS product, ld.mmnrnj       AS nominal_yield, ld.real_deposit_yield AS real_yield FROM latest_dep ld
""")
job.write_parquet("deposit_vs_loan", "SELECT * FROM deposit_vs_loan")

job.update_progress("running_analysis", progress_pct=80,
                    progress_message="Writing trend parquet…")

conn.execute("""
    CREATE OR REPLACE TABLE trend AS
    WITH ordered AS (
        SELECT date, real_rate_10y,
               ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
        FROM metrics
        WHERE real_rate_10y IS NOT NULL
    )
    SELECT
        AVG(CASE WHEN rn <=  6 THEN real_rate_10y END) AS recent_6m_avg,
        AVG(CASE WHEN rn >   6 AND rn <= 12 THEN real_rate_10y END) AS prior_6m_avg,
        AVG(CASE WHEN rn <=  6 THEN real_rate_10y END)
          - AVG(CASE WHEN rn > 6 AND rn <= 12 THEN real_rate_10y END) AS trend_delta,
        CASE WHEN AVG(CASE WHEN rn <= 6 THEN real_rate_10y END)
                > AVG(CASE WHEN rn > 6 AND rn <= 12 THEN real_rate_10y END)
             THEN 'up' ELSE 'down' END AS trend_direction
    FROM ordered
""")
job.write_parquet("trend", "SELECT * FROM trend")

job.update_progress("running_analysis", progress_pct=88,
                    progress_message="Writing kpis parquet…")

conn.execute("""
    CREATE OR REPLACE TABLE kpis AS
    WITH latest AS (
        SELECT *
        FROM metrics
        WHERE real_rate_10y IS NOT NULL
        ORDER BY date DESC LIMIT 1
    ),
    dvl AS (
        SELECT
            MAX(CASE WHEN product='mortgage_30y'    THEN nominal_yield END) AS mortgage_nominal,
            MAX(CASE WHEN product='deposit_savings' THEN nominal_yield END) AS deposit_nominal
        FROM deposit_vs_loan
    )
    SELECT
        l.real_rate_10y     AS current_real_rate_10y,
        l.inflation_yoy     AS current_inflation_yoy,
        l.dgs10             AS current_dgs10,
        dvl.mortgage_nominal - dvl.deposit_nominal AS deposit_loan_gap_pct,
        l.date              AS last_observation_date
    FROM latest l, dvl
""")
job.write_parquet("kpis", "SELECT * FROM kpis")

# Surface KPI table to log so the run page shows numbers even before the
# dashboard is opened.
kpi_rows = conn.execute("SELECT * FROM kpis").fetchall()
if kpi_rows:
    r = kpi_rows[0]
    job.table(
        "kpi_summary",
        "Purchasing Power Monitor — Current KPIs",
        ["Last Obs", "10Y Nominal (%)", "Inflation YoY (%)", "Real Rate 10Y (%)", "Deposit-Loan Gap (pp)"],
        [[str(r[4]),
          f"{r[2]:.2f}" if r[2] is not None else "N/A",
          f"{r[1]:.2f}" if r[1] is not None else "N/A",
          f"{r[0]:.2f}" if r[0] is not None else "N/A",
          f"{r[3]:.2f}" if r[3] is not None else "N/A"]]
    )

job.update_progress("running_analysis", progress_pct=95, progress_message="Uploading dashboard…",
    lineage={
        "inputs": ["bedrock.finance.fred_macro"],
        "outputs": [
            "analytics/bedrock/JOB_ID/data/kpis.parquet",
            "analytics/bedrock/JOB_ID/data/fisher_gap.parquet",
            "analytics/bedrock/JOB_ID/data/deposit_vs_loan.parquet",
            "analytics/bedrock/JOB_ID/data/trend.parquet",
        ],
    },
)

job.write_dashboard("dashboard/index.md")
job.complete()
