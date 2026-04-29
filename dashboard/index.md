---
title: "Purchasing Power Monitor — Real Rates Dashboard"
description: "Fisher Gap analysis: real yields across 10Y Treasuries, mortgages, and savings deposits."
---

# 📉 Purchasing Power Monitor

> Real rates tell savers and investors the **after-inflation return** on money.
> A negative real rate means inflation is eroding purchasing power faster than assets grow.

---

## Current Snapshot

{% big_value
   label="Real Rate — 10Y Treasury"
   query="SELECT ROUND(current_real_rate_10y, 2) || '%' AS value FROM read_parquet('kpis.parquet') LIMIT 1"
   column="value"
   description="10Y nominal Treasury minus YoY CPI inflation (Fisher gap)"
   color_column="current_real_rate_10y"
   positive_good=true
/%}

{% big_value
   label="CPI Inflation YoY"
   query="SELECT ROUND(current_inflation_yoy, 2) || '%' AS value FROM read_parquet('kpis.parquet') LIMIT 1"
   column="value"
   description="Year-over-year change in CPIAUCSL (all-items CPI)"
/%}

{% big_value
   label="10Y Treasury Nominal"
   query="SELECT ROUND(current_dgs10, 2) || '%' AS value FROM read_parquet('kpis.parquet') LIMIT 1"
   column="value"
   description="DGS10 — US 10-Year Constant Maturity Treasury yield"
/%}

{% big_value
   label="Real Rate Trend (6M vs Prior 6M)"
   query="SELECT CASE WHEN trend_delta >= 0 THEN '▲ Rising (' || ROUND(trend_delta,2) || ' pp)' ELSE '▼ Falling (' || ROUND(trend_delta,2) || ' pp)' END AS value, trend_direction FROM read_parquet('trend.parquet') LIMIT 1"
   column="value"
   description="Average real 10Y rate: last 6 months vs prior 6 months"
   color_rule="IF(trend_direction='up','green','red')"
/%}

---

## Fisher Gap — Nominal Treasury vs CPI Inflation

> The gap between the orange and blue lines is the **real rate**. When the lines cross (inflation > nominal), savers lose purchasing power.

{% line_chart
   title="Fisher Gap — Nominal vs Inflation"
   query="SELECT date, ROUND(dgs10,2) AS \"10Y Treasury (Nominal %)\", ROUND(inflation_yoy,2) AS \"CPI Inflation YoY %\" FROM read_parquet('fisher_gap.parquet') ORDER BY date"
   x="date"
   series=["10Y Treasury (Nominal %)", "CPI Inflation YoY %"]
   x_label="Date"
   y_label="Rate (%)"
/%}

---

## Real 10Y Treasury Rate Over Time

> Values below **zero** indicate negative real yields — Treasury holders earn less than inflation.

{% line_chart
   title="Real 10Y Treasury Rate (Fisher Gap)"
   query="SELECT date, ROUND(real_rate_10y,2) AS \"Real 10Y Rate %\" FROM read_parquet('fisher_gap.parquet') ORDER BY date"
   x="date"
   series=["Real 10Y Rate %"]
   x_label="Date"
   y_label="Real Rate (%)"
   reference_line=0
/%}

---

## Real Yields: Deposit Savings vs 30Y Mortgage

> Current snapshot: nominal and real yield by product. A negative real deposit yield means savings accounts are paying savers less than the rate of inflation.

{% bar_chart
   title="Real Yields: Deposit vs Mortgage (Current)"
   query="SELECT product, ROUND(nominal_yield,2) AS \"Nominal Yield %\", ROUND(real_yield,2) AS \"Real Yield %\" FROM read_parquet('deposit_vs_loan.parquet')"
   x="product"
   series=["Nominal Yield %", "Real Yield %"]
   x_label="Product"
   y_label="Yield (%)"
/%}

---

## What Does a Negative Real Rate Mean for Savers?

When the **real rate is negative**, every dollar sitting in a savings account, money market fund,
or Treasury bond is **losing purchasing power** in real terms — even if the nominal balance grows.
For example, a 2 % deposit yield with 5 % inflation implies a **−3 % real return**: you can buy
3 % less with your savings after one year.

This environment typically benefits **borrowers** (mortgages become cheaper in real terms) and
**hard assets** (real estate, commodities, inflation-linked bonds like TIPS) while penalising
**cash-equivalent savers**. Investors who ignore the Fisher gap risk silently underperforming
inflation over multi-year holding periods.

**Data sources:** FRED series — DGS10 (10Y Treasury), CPIAUCSL (CPI All Items), FEDFUNDS (Fed Funds),
MORTGAGE30US (30-Year Fixed Mortgage), MMNRNJ (Retail Money Market Funds). Updated monthly.
