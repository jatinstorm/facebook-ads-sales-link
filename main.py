# main.py
from get_facebook_ads import get_facebook_ads
from get_asin import get_asin
from get_sales import get_sales
from bq import get_client
from pandas_gbq import to_gbq
import pandas as pd

def run_pipeline():
    """Run the daily ads + sales pipeline."""

    # 1. Get Facebook ads
    ads = get_facebook_ads()

    if ads.empty:
        print("No Facebook ads data for yesterday.")
        return {"status": "no_data", "rows": 0}

    # 2. Clean Edition_ID
    ads["Edition_ID"] = (
        ads["Edition_ID"]
        .astype(str)
        .str.replace(",", "", regex=False)
    )
    ads["Edition_ID"] = pd.to_numeric(
        ads["Edition_ID"], errors="coerce"
    ).astype("Int64")

    # 3. Get ASIN / ISBN mapping
    edition_ids = ads["Edition_ID"].dropna().unique().tolist()
    asin_map = get_asin(edition_ids)
    asin_map["Edition_ID"] = asin_map["Edition_ID"].astype("Int64")
    ads = ads.merge(asin_map, on="Edition_ID", how="left")

    # 4. Prepare ASIN / ISBN lists
    asins = ads["ASIN"].dropna().unique().tolist()
    isbns = ads["paperback_isbn"].dropna().unique().tolist()
    ebook_df, kenp_df, paperback_df = get_sales(asins, isbns)

    # 5. Merge ebook sales (by ASIN + date + Territory)
    final = ads.merge(
        ebook_df,
        left_on=["ASIN", "date_start", "Territory"],
        right_on=["ASIN", "sale_date", "Territory"],
        how="left"
    )

    # 6. Merge KENP (by ASIN + date + Territory)
    final = final.merge(
        kenp_df,
        left_on=["ASIN", "date_start", "Territory"],
        right_on=["ASIN", "sale_date", "Territory"],
        how="left",
        suffixes=("", "_kenp")
    )

    # 7. Merge paperback sales (by ISBN + date + Territory)
    final = final.merge(
        paperback_df,
        left_on=["paperback_isbn", "date_start", "Territory"],
        right_on=["paperback_isbn", "sale_date", "Territory"],
        how="left",
        suffixes=("", "_paperback")
    )

    # Keep one sale_date column
    final["sale_date"] = final["sale_date"].fillna(
        final.get("sale_date_kenp")
    ).fillna(
        final.get("sale_date_paperback")
    )

    # 8. Fill missing values
    final["ebook_units"] = final["ebook_units"].fillna(0)
    final["paperback_units"] = final["paperback_units"].fillna(0)
    final["kenp"] = final["kenp"].fillna(0)
    final["ebook_revenue"] = final["ebook_revenue"].fillna(0)
    final["paperback_revenue"] = final["paperback_revenue"].fillna(0)
    final["kenp_revenue"] = final["kenp_revenue"].fillna(0)

    # 9. Deduplicate sales across adsets
    # Ebook: dedup by ASIN + date + Territory
    ebook_dup = final.duplicated(
        subset=["ASIN", "date_start", "Territory"], keep="first"
    )
    final.loc[ebook_dup, ["ebook_units", "ebook_revenue"]] = 0

    # KENP: dedup by ASIN + date + Territory
    kenp_dup = final.duplicated(
        subset=["ASIN", "date_start", "Territory"], keep="first"
    )
    final.loc[kenp_dup, ["kenp", "kenp_revenue"]] = 0
    # Paperback: dedup by ISBN + date + Territory
    paperback_dup = final.duplicated(
        subset=["paperback_isbn", "date_start", "Territory"], keep="first"
    )
    final.loc[paperback_dup, ["paperback_units", "paperback_revenue"]] = 0

    # 10. Fix data types
    final["paperback_isbn"] = final["paperback_isbn"].fillna("").astype(str)
    final["ASIN"] = final["ASIN"].fillna("").astype(str)
    final["Title"] = final["Title"].fillna("").astype(str)
    final["date_start"] = pd.to_datetime(final["date_start"])
    final["sale_date"] = pd.to_datetime(final["sale_date"], errors="coerce")

    for col in final.select_dtypes(include=["object"]).columns:
        final[col] = final[col].astype(str)

    # 11. Keep only schema columns
    final = final[
        [
            "Edition_ID", "Territory", "Targeting_type", "Targeting",
            "Age_range", "date_start", "sale_date", "spend", "clicks",
            "cpc", "ctr", "impressions", "ASIN", "adset_name", "Title",
            "paperback_isbn", "ebook_units", "paperback_units", "kenp",
            "ebook_revenue", "paperback_revenue", "Genre", "Genre_Subgenre",
            "kenp_revenue"
        ]
    ]

    # 12. Upload to BigQuery
    client = get_client()
    client.query("""
    DELETE FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    WHERE date_start = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """).result()

    to_gbq(
        final,
        "facebook_ads.ads_sales_analytics",
        project_id="marketing-489109",
        if_exists="append"
    )

    row_count = len(final)
    print(f"Pipeline complete: {row_count} rows uploaded.")
    return {"status": "success", "rows": row_count}


if __name__ == "__main__":
    run_pipeline()