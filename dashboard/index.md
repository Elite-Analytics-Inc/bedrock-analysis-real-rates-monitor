---
title: Purchasing Power Monitor
---

# 📉 Purchasing Power Monitor

> **Real rates** tell savers and investors the *after-inflation* return on money. A negative real rate means inflation is eroding purchasing power faster than nominal yields can grow it.

```sql kpis
SELECT * FROM kpis
```

```sql fisher
SELECT date, dgs10, inflation_yoy, real_rate_10y FROM fisher_gap ORDER BY date
```

```sql dvl
SELECT product, nominal_yield, real_yield FROM deposit_vs_loan
```

```sql trend
SELECT recent_6m_avg, prior_6m_avg, trend_delta, trend_direction FROM trend
```

## Current Snapshot

{% big_value data="$kpis" value="current_real_rate_10y" title="Real Rate — 10Y" fmt="num2" suffix="%" /%}
{% big_value data="$kpis" value="current_inflation_yoy" title="CPI Inflation YoY" fmt="num2" suffix="%" /%}
{% big_value data="$kpis" value="current_dgs10" title="10Y Treasury (Nominal)" fmt="num2" suffix="%" /%}
{% big_value data="$trend" value="trend_delta" title="6-Mo Trend (Δ)" fmt="num2" suffix=" pp" /%}

## Fisher Gap — Nominal Treasury vs CPI Inflation

The gap between the **DGS10 (orange)** and **inflation (red)** lines is the real rate. Where inflation crosses *above* nominal, savers lose purchasing power.

{% line_chart data="$fisher" x="date" y=["dgs10", "inflation_yoy"] title="10Y Treasury vs CPI YoY" colors=["#f97316", "#dc2626"] yAxisTitle="%" yFmt="num1" chartAreaHeight=240 /%}

## Real 10-Year Treasury Rate

Values *below zero* mean Treasury holders are earning **less than inflation**.

{% line_chart data="$fisher" x="date" y="real_rate_10y" title="Real 10Y Rate (DGS10 − CPI YoY)" colors=["#16a34a"] yAxisTitle="%" yFmt="num1" chartAreaHeight=220 /%}

## Real Yields: Deposit Savings vs 30-Year Mortgage

Most recent observation per product. Mortgages are the cost of borrowing in real terms; the deposit yield is what your savings actually earn after inflation.

{% grid cols=2 %}

{% bar_chart data="$dvl" x="product" y=["nominal_yield", "real_yield"] title="Nominal vs Real Yield (%)" colors=["#3b82f6", "#16a34a"] yFmt="num2" /%}

{% data_table data="$dvl" title="Yields by Product" rowShading=true /%}

{% /grid %}

## What a Negative Real Rate Means for Savers

When the real rate is negative, every dollar in a savings account, money market fund, or Treasury bond is **losing purchasing power in real terms** — even as the nominal balance grows. A 2 % deposit yield with 5 % inflation is a *−3 %* real return: you can buy 3 % less with your savings after one year.

This environment typically benefits **borrowers** (mortgages get cheaper in real terms) and **hard assets** (real estate, commodities, TIPS). Cash-equivalent savers silently underperform inflation over multi-year holding periods if they don't act.

---

*Data: FRED — DGS10, CPIAUCSL, FEDFUNDS, MORTGAGE30US, MMNRNJ. CPI inflation is YoY change of CPIAUCSL with a 12-month lag. Updated on every analysis run.*
