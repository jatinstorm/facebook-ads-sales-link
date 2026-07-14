# backfill_range.py
"""
Replays the daily main.py pipeline (Facebook ads + sales/KENP/paperback join)
for a RANGE of dates, instead of just "yesterday".

Use this to fill gaps in `marketing-489109.facebook_ads.ads_sales_analytics`
for days the daily pipeline missed (e.g. when FB ads weren't importing).

For each date it: DELETEs that date's rows, then re-builds and appends them.
Safe to re-run — idempotent per date.

Run:  python backfill_range.py
"""
from datetime import date, timedelta

import pandas as pd
from pandas_gbq import to_gbq

from get_asin import get_asin
from bq import get_client

# ── Config ──────────────────────────────────────────────
BACKFILL_FROM = date(2026, 6, 19)
BACKFILL_TO   = date(2026, 6, 21)   # inclusive
# ────────────────────────────────────────────────────────


def get_facebook_ads_for_date(client, target_date):
    query = f"""
    SELECT
        adset_name,
        Edition_ID,
        Territory,
        Targeting_type,
        Targeting,
        Age_range,
        DATE(date_start) AS date_start,
        SUM(spend) AS spend,
        SUM(clicks) AS clicks,
        AVG(cpc) AS cpc,
        AVG(ctr) AS ctr,
        SUM(impressions) AS impressions
    FROM `marketing-489109.facebook_ads.facebook_ads`
    WHERE DATE(date_start) = '{target_date}' AND Territory NOT IN ('USGB', 'GBUS')
    GROUP BY
        Edition_ID, Territory, Targeting_type, Targeting,
        Age_range, adset_name, date_start
    """
    return client.query(query).to_dataframe()


def get_sales_for_date(client, target_date, asins, isbns):
    asin_list = ",".join([f"'{a}'" for a in asins]) or "''"
    isbn_list = ",".join([f"'{i}'" for i in isbns]) or "''"

    ebook_query = f"""
    SELECT
        ASIN,
        Royalty_Date AS sale_date,
        CASE WHEN Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS Territory,
        SUM(Net_Units_Sold) AS ebook_units,
        SUM(Royalty_GBP) AS ebook_revenue
    FROM `storm-pub-amazon-sales.daily_sales.daily_sales_ebook_agg`
    WHERE Royalty_Date = '{target_date}'
    AND ASIN IN ({asin_list})
    AND Royalty_GBP > 0
    GROUP BY ASIN, sale_date, Territory
    """

    paperback_query = f"""
    SELECT
        CAST(ISBN AS STRING) AS paperback_isbn,
        Royalty_Date AS sale_date,
        CASE WHEN Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS Territory,
        SUM(Net_Units_Sold) AS paperback_units,
        SUM(Royalty_GBP) AS paperback_revenue
    FROM `storm-pub-amazon-sales.daily_sales.daily_sales_paperback_agg`
    WHERE Royalty_Date = '{target_date}'
    AND CAST(ISBN AS STRING) IN ({isbn_list})
    GROUP BY ISBN, sale_date, Territory
    """

    kenp_query = f"""
    SELECT
        ASIN,
        Date AS sale_date,
        CASE WHEN Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS Territory,
        SUM(KENP) AS kenp,
        SUM(Royalty_GBP) AS kenp_revenue
    FROM `storm-pub-amazon-sales.daily_sales.daily_sales_kenp_agg`
    WHERE Date = '{target_date}'
    AND ASIN IN ({asin_list})
    GROUP BY ASIN, sale_date, Territory
    """

    ebook_df = client.query(ebook_query).to_dataframe()
    kenp_df = client.query(kenp_query).to_dataframe()
    paperback_df = client.query(paperback_query).to_dataframe()
    return ebook_df, kenp_df, paperback_df


def process_date(client, target_date):
    """Replicates main.run_pipeline() for one specific date."""
    print(f"\n=== {target_date} ===")

    # 1. Get Facebook ads
    ads = get_facebook_ads_for_date(client, target_date)
    if ads.empty:
        print("  No Facebook ads data — skipping.")
        return 0

    # 2. Clean Edition_ID
    ads["Edition_ID"] = ads["Edition_ID"].astype(str).str.replace(",", "", regex=False)
    ads["Edition_ID"] = pd.to_numeric(ads["Edition_ID"], errors="coerce").astype("Int64")

    # 3. Get ASIN / ISBN mapping
    edition_ids = ads["Edition_ID"].dropna().unique().tolist()
    asin_map = get_asin(edition_ids)
    asin_map["Edition_ID"] = asin_map["Edition_ID"].astype("Int64")
    ads = ads.merge(asin_map, on="Edition_ID", how="left")

    # 4. Prepare ASIN / ISBN lists
    asins = ads["ASIN"].dropna().unique().tolist()
    isbns = ads["paperback_isbn"].dropna().unique().tolist()
    ebook_df, kenp_df, paperback_df = get_sales_for_date(client, target_date, asins, isbns)

    # 5. Merge ebook sales
    final = ads.merge(
        ebook_df,
        left_on=["ASIN", "date_start", "Territory"],
        right_on=["ASIN", "sale_date", "Territory"],
        how="left",
    )

    # 6. Merge KENP
    final = final.merge(
        kenp_df,
        left_on=["ASIN", "date_start", "Territory"],
        right_on=["ASIN", "sale_date", "Territory"],
        how="left",
        suffixes=("", "_kenp"),
    )

    # 7. Merge paperback sales
    final = final.merge(
        paperback_df,
        left_on=["paperback_isbn", "date_start", "Territory"],
        right_on=["paperback_isbn", "sale_date", "Territory"],
        how="left",
        suffixes=("", "_paperback"),
    )

    # Keep one sale_date column
    final["sale_date"] = (
        final["sale_date"]
        .fillna(final.get("sale_date_kenp"))
        .fillna(final.get("sale_date_paperback"))
    )

    # 8. Fill missing values
    for col in ["ebook_units", "paperback_units", "kenp",
                "ebook_revenue", "paperback_revenue", "kenp_revenue"]:
        final[col] = final[col].fillna(0)

    # 9. Deduplicate sales across adsets
    ebook_dup = final.duplicated(subset=["ASIN", "date_start", "Territory"], keep="first")
    final.loc[ebook_dup, ["ebook_units", "ebook_revenue"]] = 0
    kenp_dup = final.duplicated(subset=["ASIN", "date_start", "Territory"], keep="first")
    final.loc[kenp_dup, ["kenp", "kenp_revenue"]] = 0
    paperback_dup = final.duplicated(subset=["paperback_isbn", "date_start", "Territory"], keep="first")
    final.loc[paperback_dup, ["paperback_units", "paperback_revenue"]] = 0

    # 10. Fix data types
    final["paperback_isbn"] = final["paperback_isbn"].fillna("").astype(str)
    final["ASIN"] = final["ASIN"].fillna("").astype(str)
    final["Title"] = final["Title"].fillna("").astype(str)
    final["date_start"] = pd.to_datetime(final["date_start"])
    final["sale_date"] = pd.to_datetime(final["sale_date"], errors="coerce")
    for col in final.select_dtypes(include=["object"]).columns:
        if col == "Series":
            continue
        final[col] = final[col].astype(str)

    # 11. Keep only schema columns
    final = final[[
        "Edition_ID", "Territory", "Targeting_type", "Targeting",
        "Age_range", "date_start", "sale_date", "spend", "clicks",
        "cpc", "ctr", "impressions", "ASIN", "adset_name", "Title",
        "paperback_isbn", "ebook_units", "paperback_units", "kenp",
        "ebook_revenue", "paperback_revenue", "Genre", "Genre_Subgenre",
        "kenp_revenue", "Series", "Series_No",
    ]]

    # 12. Upload — delete this date's rows first, then append
    client.query(f"""
    DELETE FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    WHERE date_start = '{target_date}'
    """).result()

    to_gbq(
        final,
        "facebook_ads.ads_sales_analytics",
        project_id="marketing-489109",
        if_exists="append",
    )
    print(f"  Uploaded {len(final)} rows.")
    return len(final)


def run_backfill():
    client = get_client()
    total = 0
    d = BACKFILL_FROM
    while d <= BACKFILL_TO:
        total += process_date(client, d)
        d += timedelta(days=1)

    # Series metadata repair (same as main.py, applied once at the end)
    print("\nRunning series metadata repair...")
    client.query("""
    MERGE `marketing-489109.facebook_ads.ads_sales_analytics` a
    USING (
      SELECT
        e.ID AS Edition_ID,
        MIN(e.Series) AS Series,
        MIN(SAFE_CAST(REGEXP_EXTRACT(e.Series_No, r'(\\d+)') AS INT64)) AS Series_No
      FROM `storm-pub-amazon-sales.airtable.awe_editions` e
      GROUP BY e.ID
    ) ed
    ON a.Edition_ID = ed.Edition_ID
    WHEN MATCHED AND (a.Series IS NULL OR a.Series_No IS NULL) THEN UPDATE SET
      Series = ed.Series,
      Series_No = ed.Series_No
    """).result()
    print("Repair complete.")

    print(f"\nBackfill complete: {total} rows across {BACKFILL_FROM} .. {BACKFILL_TO}.")


if __name__ == "__main__":
    run_backfill()
