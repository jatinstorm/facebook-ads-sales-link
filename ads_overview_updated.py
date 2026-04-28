# ads_overview.py
"""
Generates the ads overview dashboard data.

Terminology:
  - "Campaign" = one book × one territory, regardless of adset count.
    Multiple adsets for the same book in the same territory roll up into
    a single campaign row.
  - "Running" = the campaign had spend on the latest day we have data for.

Run:  python ads_overview.py

Produces ads_overview_data.json with the shape the dashboard expects.

UPDATES (Month Tab Fix):
  - Now exports 'month_campaigns' array containing ALL campaigns from current month
    (both running and ended) to enable proper filtering in the dashboard
  - This allows the Month tab to show ended campaigns and apply filters correctly
"""

import json
import math
import pandas as pd
from datetime import datetime, date
from bq import get_client

# ─────────────────────────────────────────────
# STEP 1 — Fetch raw data from BigQuery
# ─────────────────────────────────────────────
def get_data():
    client = get_client()

    # Running campaigns: books × territories whose current run reaches
    # the latest day in the table. Aggregates all adsets together so one
    # row = one campaign (book × territory) in its active run.
    running_query = f"""
    WITH run_dates AS (
      SELECT Edition_ID, Territory, date_start,
        DATE_DIFF(date_start, LAG(date_start) OVER (
          PARTITION BY Edition_ID, Territory ORDER BY date_start
        ), DAY) AS gap
      FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    ),
    run_starts AS (
      SELECT Edition_ID, Territory, MAX(date_start) AS run_start
      FROM run_dates
      WHERE gap IS NULL OR gap > 1
      GROUP BY Edition_ID, Territory
    ),
    latest_date AS (
      SELECT MAX(date_start) AS max_date
      FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    ),
    -- Only keep campaigns whose current run includes the latest data day
    active_campaigns AS (
      SELECT DISTINCT rs.Edition_ID, rs.Territory, rs.run_start
      FROM run_starts rs, latest_date ld
      WHERE EXISTS (
        SELECT 1 FROM `marketing-489109.facebook_ads.ads_sales_analytics` x
        WHERE x.Edition_ID = rs.Edition_ID
          AND x.Territory = rs.Territory
          AND x.date_start = ld.max_date
      )
    )
    SELECT
      a.Title, a.Edition_ID, a.Territory,
      a.Series, a.Series_No, a.Genre, a.Genre_Subgenre,
      e.Cover_Author,
      ac.run_start,
      COUNT(DISTINCT a.date_start) AS run_days,
      MAX(a.date_start) AS last_active_date,
      SUM(a.spend) AS spend,
      SUM(a.clicks) AS clicks,
      SUM(a.impressions) AS impressions,
      SUM(a.ebook_units) AS ebook_units,
      SUM(a.paperback_units) AS paperback_units,
      SUM(a.kenp) AS kenp,
      SUM(a.ebook_revenue) AS ebook_revenue,
      SUM(a.paperback_revenue) AS paperback_revenue,
      SUM(a.kenp_revenue) AS kenp_revenue,
      MAX(m.ebook_value_per_unit) AS ebook_value_per_unit,
      MAX(m.kenp_value_per_unit) AS kenp_value_per_unit,
      MAX(m.pod_value_per_unit) AS pod_value_per_unit
    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a
    JOIN active_campaigns ac
      ON a.Edition_ID = ac.Edition_ID
      AND a.Territory = ac.Territory
      AND a.date_start >= ac.run_start
      AND NOT (a.ebook_units > 0 AND a.ebook_revenue = 0)
    LEFT JOIN `marketing-489109.facebook_ads.series_multipliers` m
      ON a.Series = m.Series AND a.Territory = m.Territory
    LEFT JOIN `storm-pub-amazon-sales.airtable.awe_editions` e
      ON a.Edition_ID = e.ID
    GROUP BY a.Title, a.Edition_ID, a.Territory, a.Series, a.Series_No,
             a.Genre, a.Genre_Subgenre, e.Cover_Author, ac.run_start
    """

    # Current calendar month: one row per campaign (book × territory)
    month_query = f"""
    SELECT
      a.Title, a.Edition_ID, a.Territory,
      a.Series, a.Series_No, a.Genre, a.Genre_Subgenre,
      e.Cover_Author,
      SUM(a.spend) AS spend,
      SUM(a.clicks) AS clicks,
      SUM(a.impressions) AS impressions,
      SUM(a.ebook_units) AS ebook_units,
      SUM(a.paperback_units) AS paperback_units,
      SUM(a.kenp) AS kenp,
      SUM(a.ebook_revenue) AS ebook_revenue,
      SUM(a.paperback_revenue) AS paperback_revenue,
      SUM(a.kenp_revenue) AS kenp_revenue,
      MAX(m.ebook_value_per_unit) AS ebook_value_per_unit,
      MAX(m.kenp_value_per_unit) AS kenp_value_per_unit,
      MAX(m.pod_value_per_unit) AS pod_value_per_unit
    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a
    LEFT JOIN `marketing-489109.facebook_ads.series_multipliers` m
      ON a.Series = m.Series AND a.Territory = m.Territory
    LEFT JOIN `storm-pub-amazon-sales.airtable.awe_editions` e
      ON a.Edition_ID = e.ID
    WHERE DATE_TRUNC(a.date_start, MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
     AND NOT (a.ebook_units > 0 AND a.ebook_revenue = 0)
    GROUP BY a.Title, a.Edition_ID, a.Territory, a.Series, a.Series_No,
             a.Genre, a.Genre_Subgenre, e.Cover_Author
    """

    # Monthly history: last 12 calendar months, one row per month × campaign
    # NOW INCLUDING clicks and impressions
    history_query = f"""
    SELECT
      DATE_TRUNC(a.date_start, MONTH) AS year_month,
      a.Title, a.Edition_ID, a.Territory,
      a.Series, a.Series_No, a.Genre, a.Genre_Subgenre,
      e.Cover_Author,
      SUM(a.spend) AS spend,
      SUM(a.clicks) AS clicks,
      SUM(a.impressions) AS impressions,
      SUM(a.ebook_units) AS ebook_units,
      SUM(a.paperback_units) AS paperback_units,
      SUM(a.kenp) AS kenp,
      SUM(a.ebook_revenue) AS ebook_revenue,
      SUM(a.paperback_revenue) AS paperback_revenue,
      SUM(a.kenp_revenue) AS kenp_revenue,
      MAX(m.ebook_value_per_unit) AS ebook_value_per_unit,
      MAX(m.kenp_value_per_unit) AS kenp_value_per_unit,
      MAX(m.pod_value_per_unit) AS pod_value_per_unit
    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a
    LEFT JOIN `marketing-489109.facebook_ads.series_multipliers` m
      ON a.Series = m.Series AND a.Territory = m.Territory
    LEFT JOIN `storm-pub-amazon-sales.airtable.awe_editions` e
      ON a.Edition_ID = e.ID
    WHERE a.date_start >= '2020-01-01'
     AND NOT (a.ebook_units > 0 AND a.ebook_revenue = 0)
    GROUP BY year_month, a.Title, a.Edition_ID, a.Territory, a.Series, a.Series_No,
             a.Genre, a.Genre_Subgenre, e.Cover_Author
    """

    running_df = client.query(running_query).to_dataframe()
    month_df = client.query(month_query).to_dataframe()
    history_df = client.query(history_query).to_dataframe()

    return running_df, month_df, history_df


# ─────────────────────────────────────────────
# STEP 2 — Helpers: money + ROI + row shaping
# ─────────────────────────────────────────────
def _num(v):
    """Safely coerce to float, treating NaN/None/NA as 0."""
    if pd.isna(v):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _roi(profit, spend):
    """ROI as a percentage. None if spend is 0."""
    if spend is None or spend == 0:
        return None
    return round((profit / spend) * 100, 1)


def _round(v, n=2):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return round(v, n)


def _str_or_none(v):
    """Return a string, or None for any flavour of missing."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    return s if s else None


def _campaign_row(row):
    """Convert one DataFrame row into a campaign dict (book × territory)."""
    spend = _num(row.get("spend"))
    clicks = _num(row.get("clicks"))
    impressions = _num(row.get("impressions"))
    
    ebook_units = _num(row.get("ebook_units"))
    paperback_units = _num(row.get("paperback_units"))
    kenp = _num(row.get("kenp"))
    
    ebook_rev = _num(row.get("ebook_revenue"))
    pb_rev = _num(row.get("paperback_revenue"))
    kenp_rev = _num(row.get("kenp_revenue"))
    revenue = ebook_rev + pb_rev + kenp_rev
    pub_rev = revenue * 0.5
    gross_profit = pub_rev - spend

    # Calculate CPC and CTR
    cpc = (spend / clicks) if clicks > 0 else None
    ctr = ((clicks / impressions) * 100) if impressions > 0 else None

    # Calculate series profit CORRECTLY (only for Book 1)
    series_no_raw = row.get("Series_No")
    series_no = None if pd.isna(series_no_raw) else int(series_no_raw)
    
    series_profit = None
    series_roi = None
    
    if series_no == 1:
        # Get the multiplier values
        ebook_value = _num(row.get("ebook_value_per_unit"))
        kenp_value = _num(row.get("kenp_value_per_unit"))
        pod_value = _num(row.get("pod_value_per_unit"))
        
        # Calculate series value: total_units * value_per_unit for each channel
        series_value = (
            (ebook_units * ebook_value) +
            (paperback_units * pod_value) +
            (kenp * kenp_value)
        ) * 0.5  # Publisher's 50% share
        
        # Series profit = series_value - spend
        series_profit = series_value - spend
        series_roi = _roi(series_profit, spend)

    edition_raw = row.get("Edition_ID")
    edition_id = None if pd.isna(edition_raw) else int(edition_raw)

    run_days = None
    if "run_days" in row and not pd.isna(row.get("run_days")):
        run_days = int(row["run_days"])

    return {
        "title": _str_or_none(row.get("Title")),
        "edition_id": edition_id,
        "territory": _str_or_none(row.get("Territory")),
        "series": _str_or_none(row.get("Series")),
        "series_no": series_no,
        "genre": _str_or_none(row.get("Genre")),
        "genre_subgenre": _str_or_none(row.get("Genre_Subgenre")),
        "author": _str_or_none(row.get("Cover_Author")),
        "run_days": run_days,
        "spend": _round(spend),
        "clicks": _round(clicks, 0),
        "impressions": _round(impressions, 0),
        "cpc": _round(cpc, 3),
        "ctr": _round(ctr, 2),
        "revenue": _round(revenue),
        "pub_revenue": _round(pub_rev),
        "gross_profit": _round(gross_profit),
        "gross_roi": _roi(gross_profit, spend),
        "series_profit": _round(series_profit) if series_profit is not None else None,
        "series_roi": series_roi,
    }


def _summarize(rows):
    """Aggregate a list of campaign dicts into a summary block."""
    spend = sum(_num(r["spend"]) for r in rows)
    clicks = sum(_num(r["clicks"]) for r in rows)
    impressions = sum(_num(r["impressions"]) for r in rows)
    revenue = sum(_num(r["revenue"]) for r in rows)
    pub_rev = sum(_num(r["pub_revenue"]) for r in rows)
    gross_profit = sum(_num(r["gross_profit"]) for r in rows)

    # Calculate aggregate CPC and CTR
    cpc = (spend / clicks) if clicks > 0 else None
    ctr = ((clicks / impressions) * 100) if impressions > 0 else None

    book1 = [r for r in rows if r.get("series_no") == 1 and r.get("series_profit") is not None]
    standalone = [r for r in rows if not (r.get("series_no") == 1 and r.get("series_profit") is not None)]

    series_spend = sum(_num(r["spend"]) for r in book1)
    series_profit = sum(_num(r["series_profit"]) for r in book1)

    standalone_spend = sum(_num(r["spend"]) for r in standalone)
    standalone_gross_profit = sum(_num(r["gross_profit"]) for r in standalone)

    return {
        "spend": _round(spend),
        "clicks": _round(clicks, 0),
        "impressions": _round(impressions, 0),
        "cpc": _round(cpc, 3),
        "ctr": _round(ctr, 2),
        "revenue": _round(revenue),
        "pub_revenue": _round(pub_rev),
        "gross_profit": _round(gross_profit),
        "gross_roi": _roi(gross_profit, spend),
        "campaigns": len(rows),
        "series_spend": _round(series_spend),
        "series_profit": _round(series_profit),
        "series_roi": _roi(series_profit, series_spend),
        "series_campaigns": len(book1),
        "standalone_spend": _round(standalone_spend),
        "standalone_profit": _round(standalone_gross_profit),
        "standalone_roi": _roi(standalone_gross_profit, standalone_spend),
        "standalone_campaigns": len(standalone),
    }


def _top_n(rows, key, n=10, book1_only=False):
    """Top N campaigns by a given key (desc). Drops rows where key is None."""
    filtered = [r for r in rows if r.get(key) is not None]
    if book1_only:
        filtered = [r for r in filtered if r.get("series_no") == 1]
    return sorted(filtered, key=lambda r: r[key], reverse=True)[:n]


def _month_label(d):
    if isinstance(d, str):
        d = datetime.fromisoformat(d).date()
    return d.strftime("%B %Y")


# ─────────────────────────────────────────────
# STEP 3 — Build the output JSON structure
# ─────────────────────────────────────────────
def build_output(running_df, month_df, history_df):
    # Running campaigns → list of dicts, sorted
    running = [_campaign_row(r) for _, r in running_df.iterrows()]
    running.sort(
        key=lambda r: (
            0 if r.get("series_roi") is not None else 1,
            -(r.get("series_roi") if r.get("series_roi") is not None else 0),
            -(r.get("gross_roi") if r.get("gross_roi") is not None else 0),
        )
    )

    # Current month - ALL campaigns (running + ended)
    month_rows = [_campaign_row(r) for _, r in month_df.iterrows()]
    month_summary = _summarize(month_rows)
    month_top_series = _top_n(month_rows, "series_roi", n=10, book1_only=True)
    month_top_gross = _top_n(month_rows, "gross_roi", n=10)

    # Month by genre
    by_genre_map = {}
    for r in month_rows:
        g = r.get("genre") or "Unknown"
        by_genre_map.setdefault(g, []).append(r)
    month_by_genre = []
    for genre, rows in by_genre_map.items():
        s = _summarize(rows)
        month_by_genre.append({
            "genre": genre,
            "spend": s["spend"],
            "revenue": s["revenue"],
            "gross_profit": s["gross_profit"],
            "gross_roi": s["gross_roi"],
            "series_roi": s["series_roi"],
            "campaigns": s["campaigns"],
        })
    month_by_genre.sort(key=lambda r: -(r["spend"] or 0))

    # History — one entry per month
    history_rows = [_campaign_row(r) for _, r in history_df.iterrows()]
    for campaign, (_, df_row) in zip(history_rows, history_df.iterrows()):
        ym = df_row["year_month"]
        if hasattr(ym, "date"):
            ym = ym.date()
        campaign["_year_month"] = ym.isoformat() if hasattr(ym, "isoformat") else str(ym)

    history_by_month = {}
    for r in history_rows:
        ym = r["_year_month"]
        history_by_month.setdefault(ym, []).append(r)

    current_month_key = date.today().replace(day=1).isoformat()

    history = []
    for ym in sorted(history_by_month.keys(), reverse=True):
        if ym == current_month_key:
            continue
        rows = history_by_month[ym]
        s = _summarize(rows)
        # Keep all campaigns for author/genre/series filtering in the dashboard
        all_campaigns = [dict(r) for r in rows]
        for c in all_campaigns:
            c.pop("_year_month", None)
        # Also keep a top-5 highlight list for legacy display
        top = _top_n(rows, "series_roi", n=5, book1_only=True)
        if not top:
            top = _top_n(rows, "gross_roi", n=5)
        for t in top:
            t.pop("_year_month", None)
        history.append({
            "month": ym,
            "label": _month_label(ym),
            **s,
            "campaigns_data": all_campaigns,  # Full list for filtering
            "top_campaigns": top,             # Top 5 for display/legacy
        })

    latest_date = None
    if not running_df.empty:
        latest = running_df["last_active_date"].max()
        if latest is not None and not pd.isna(latest):
            latest_date = latest.isoformat() if hasattr(latest, "isoformat") else str(latest)

    return {
        "generated_at": datetime.now().isoformat(),
        "latest_date": latest_date,
        "month_label": _month_label(date.today().replace(day=1)),
        "month_summary": month_summary,
        "running": running,
        "month_campaigns": month_rows,  # Add ALL month campaigns for filtering
        "month_top_series_roi": month_top_series,
        "month_top_gross_roi": month_top_gross,
        "month_by_genre": month_by_genre,
        "history": history,
    }


# ─────────────────────────────────────────────
# Main entry
# ─────────────────────────────────────────────
def run_pipeline():
    print("Fetching data...")
    running_df, month_df, history_df = get_data()
    print(f"  running={len(running_df)}, month={len(month_df)}, history={len(history_df)}")

    print("Building output...")
    output = build_output(running_df, month_df, history_df)

    print("Writing ads_overview_data.json...")
    with open("ads_overview_data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    # ── Build standalone HTML and upload to GCS ──
    print("Building standalone HTML...")
    from google.cloud import storage as gcs

    with open("ads_overview.html", "r", encoding="utf-8") as f:
        html = f.read()

    json_str = json.dumps(output, indent=2, default=str)
    old_tag = '<script src="ads_overview_data.js"></script>'
    new_tag = '<script>var DATA = ' + json_str + ';</script>'
    html = html.replace(old_tag, new_tag)

    with open("ads_overview_index.html", "w", encoding="utf-8") as f:
        f.write(html)

    print("Uploading to gs://storm-series-dashboard/ads_overview.html...")
    gcs_client = gcs.Client()
    bucket = gcs_client.bucket("storm-series-dashboard")
    blob = bucket.blob("ads_overview.html")
    blob.content_type = "text/html"
    blob.upload_from_filename("ads_overview_index.html")
    print("Done! Dashboard live at https://storage.googleapis.com/storm-series-dashboard/ads_overview.html")

    ms = output["month_summary"]

    print()
    print(f"── {output['month_label']} summary ──")
    print(f"  Spend:          £{ms['spend']:,.0f}")
    print(f"  Clicks:         {ms['clicks']:,.0f}")
    print(f"  CPC:            £{ms['cpc']:.3f}")
    print(f"  CTR:            {ms['ctr']:.2f}%")
    print(f"  Revenue:        £{ms['revenue']:,.0f}")
    print(f"  Gross profit:   £{ms['gross_profit']:,.0f}  ({ms['gross_roi']}% ROI)")
    if ms['series_spend']:
        print(f"  Series spend:   £{ms['series_spend']:,.0f}  ({ms['series_campaigns']} Book 1 campaigns)")
        print(f"  Series profit:  £{ms['series_profit']:,.0f}  ({ms['series_roi']}% ROI)")
    print()
    print(f"── Running campaigns: {len(output['running'])} ──")
    for r in output["running"][:10]:
        tag = f"B1 Series ROI {r['series_roi']}%" if r.get("series_roi") is not None else f"Gross ROI {r['gross_roi']}%"
        print(f"  {r['territory']:<2}  {r['title'][:40]:<40}  {tag}")
    print()
    print(f"── History months: {len(output['history'])} ──")
    for h in output["history"][:3]:
        print(f"  {h['label']}: spend £{h['spend']:,.0f}, gross ROI {h['gross_roi']}%")

    return output


if __name__ == "__main__":
    run_pipeline()