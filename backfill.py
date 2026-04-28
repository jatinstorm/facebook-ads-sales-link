# backfill_free_sales.py
"""
Fixes historical ebook_units in ads_sales_analytics by re-fetching
from daily_sales_ebook_agg with Royalty_GBP > 0 (excludes free sales).

This corrects inflated series_profit figures caused by free Bookbub promos
where free downloads were counted at full value per unit.

Run:  python backfill_free_sales.py

Safe to re-run — uses MERGE so it won't double-update.
"""

from bq import get_client
from datetime import date

# ── Config ──────────────────────────────────────────────
# How far back to backfill. Jan 2024 spike suggests we need at least 2023-01-01.
BACKFILL_FROM = '2020-01-01'
BACKFILL_TO   = date.today().isoformat()  # up to but not including today (main.py handles today)
# ────────────────────────────────────────────────────────

def run_backfill():
    client = get_client()

    print(f"Backfilling ebook_units from {BACKFILL_FROM} to {BACKFILL_TO}...")
    print("This re-fetches from daily_sales_ebook_agg with Royalty_GBP > 0")
    print("to exclude free Bookbub promo downloads from series profit calculations.")
    print()

    corrected_subquery = f"""
        SELECT
            e.ASIN,
            eb.Royalty_Date AS date_start,
            CASE WHEN eb.Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS Territory,
            SUM(eb.Net_Units_Sold) AS paid_ebook_units,
            SUM(eb.Royalty_GBP) AS paid_ebook_revenue
        FROM `storm-pub-amazon-sales.daily_sales.daily_sales_ebook_agg` eb
        JOIN `storm-pub-amazon-sales.airtable.awe_editions` e
            ON eb.ASIN = e.ASIN
            AND e.Format = 'Ebook'
        WHERE eb.Royalty_Date BETWEEN '{BACKFILL_FROM}' AND '{BACKFILL_TO}'
          AND eb.Royalty_GBP > 0
        GROUP BY e.ASIN, eb.Royalty_Date, Territory
    """

    # Step 1: Check how many rows will be affected
    print("Step 1: Checking how many rows will be updated...")
    check_query = f"""
    SELECT COUNT(*) AS rows_to_update
    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a
    JOIN ({corrected_subquery}) c
        ON a.ASIN = c.ASIN
        AND a.date_start = c.date_start
        AND a.Territory = c.Territory
    WHERE a.ebook_units != c.paid_ebook_units
      OR a.ebook_revenue != c.paid_ebook_revenue
    """
    result = client.query(check_query).to_dataframe()
    rows_to_update = result['rows_to_update'].iloc[0]
    print(f"   {rows_to_update:,} rows have different ebook_units/revenue vs paid-only source.")

    if rows_to_update == 0:
        print("   Nothing to update — already clean!")
        return

    # Step 2: Apply the fix via MERGE
    print("Step 2: Applying fix via MERGE...")
    merge_query = f"""
    MERGE `marketing-489109.facebook_ads.ads_sales_analytics` a
    USING ({corrected_subquery}) c
        ON a.ASIN = c.ASIN
        AND a.date_start = c.date_start
        AND a.Territory = c.Territory
    WHEN MATCHED AND (
        a.ebook_units != c.paid_ebook_units
        OR a.ebook_revenue != c.paid_ebook_revenue
    ) THEN UPDATE SET
        a.ebook_units   = c.paid_ebook_units,
        a.ebook_revenue = c.paid_ebook_revenue
    """
    client.query(merge_query).result()
    print(f"   Updated {rows_to_update:,} rows.")

    # Step 3: Verify — check Jan 2024 specifically (known spike month)
    print("Step 3: Verifying Jan 2024 (known spike month)...")
    verify_query = """
    SELECT
        Title,
        Series,
        Territory,
        SUM(ebook_units) AS ebook_units,
        SUM(ebook_revenue) AS ebook_revenue
    FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    WHERE DATE_TRUNC(date_start, MONTH) = '2024-01-01'
      AND Series_No = 1
    GROUP BY Title, Series, Territory
    ORDER BY ebook_units DESC
    LIMIT 10
    """
    verify_df = client.query(verify_query).to_dataframe()
    print()
    print("Top Book 1 titles by ebook_units in Jan 2024 (should be reasonable now):")
    print(verify_df.to_string(index=False))

    print()
    print("Backfill complete! Re-run ads_overview_updated.py to regenerate the dashboard.")


if __name__ == '__main__':
    run_backfill()