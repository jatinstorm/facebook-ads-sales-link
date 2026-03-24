# series_projection.py
# ─────────────────────────────────────────────────────────
# Run this on your machine where you have BigQuery access.
# It computes readthrough rates + ROI multipliers PER CHANNEL
# (ALC Ebook / KENP / POD) and saves the results as a JSON
# file that the dashboard reads.
#
# Usage:
#   python series_projection.py
#
# Output:
#   series_projection_data.json
#   series_projection_data.js   (for local file:// usage)
# ─────────────────────────────────────────────────────────

import math
from bq import get_client
import pandas as pd
import json
from datetime import date, datetime


def json_serial(obj):
    """Safe JSON serializer (handles NaN, Infinity)."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0
        return round(obj, 4)
    return str(obj)


def clean_data(obj):
    """Recursively clean data for JSON (handles NaN, Infinity, dates)."""
    if isinstance(obj, dict):
        return {k: clean_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_data(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0
        return obj
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    else:
        return obj


def run_pipeline():
    client = get_client()

    # ──────────────────────────────────────────────
    # STEP 1: Series edition map
    # ──────────────────────────────────────────────
    print("Step 1/7: Building series edition map...")

    edition_map = client.query("""
        SELECT
            e.ID AS edition_id,
            e.Title,
            e.Series,
            SAFE_CAST(REGEXP_EXTRACT(e.Series_No, r'(\\d+)') AS INT64) AS book_number,
            e.Format,
            e.ASIN,
            e.ISBN,
            e.Genre,
            e.Genre_Subgenre,
            e.Cover_Author,
            e.Pub_Date,
            e.Word_Count
        FROM `storm-pub-amazon-sales.airtable.awe_editions` e
        WHERE e.Series IS NOT NULL
          AND e.Series_No IS NOT NULL
          AND e.Format IN ('Ebook', 'POD')
          AND SAFE_CAST(REGEXP_EXTRACT(e.Series_No, r'(\\d+)') AS INT64) IS NOT NULL
    """).to_dataframe()

    print(f"   {len(edition_map)} editions | {edition_map['Series'].nunique()} series")

    # ──────────────────────────────────────────────
    # STEP 2: Fetch all sales data
    # ──────────────────────────────────────────────
    print("Step 2/7: Fetching sales data from BigQuery...")

    ebook_asins = edition_map[edition_map['Format'] == 'Ebook']['ASIN'].dropna().unique().tolist()
    pod_isbns = edition_map[edition_map['Format'] == 'POD']['ISBN'].dropna().unique().tolist()

    asin_list = ",".join([f"'{a}'" for a in ebook_asins])
    isbn_list = ",".join([f"'{i}'" for i in pod_isbns])

    print("   Ebook sales...")
    ebook_df = client.query(f"""
        SELECT
            ASIN,
            DATE_TRUNC(Royalty_Date, MONTH) AS sale_month,
            Marketplace,
            SUM(Net_Units_Sold) AS units,
            SUM(Royalty_GBP) AS revenue_gbp
        FROM `storm-pub-amazon-sales.daily_sales.daily_sales_ebook_agg`
        WHERE ASIN IN ({asin_list}) AND Royalty_GBP > 0
        GROUP BY ASIN, sale_month, Marketplace
    """).to_dataframe()
    print(f"     {len(ebook_df)} rows")

    print("   Paperback sales...")
    paperback_df = client.query(f"""
        SELECT
            CAST(ISBN AS STRING) AS ISBN,
            DATE_TRUNC(Royalty_Date, MONTH) AS sale_month,
            Marketplace,
            SUM(Net_Units_Sold) AS units,
            SUM(Royalty_GBP) AS revenue_gbp
        FROM `storm-pub-amazon-sales.daily_sales.daily_sales_paperback_agg`
        WHERE CAST(ISBN AS STRING) IN ({isbn_list})
        GROUP BY ISBN, sale_month, Marketplace
    """).to_dataframe()
    print(f"     {len(paperback_df)} rows")

    print("   KENP reads...")
    kenp_df = client.query(f"""
        SELECT
            ASIN,
            DATE_TRUNC(Date, MONTH) AS sale_month,
            Marketplace,
            SUM(KENP) AS kenp_pages,
            SUM(Royalty_GBP) AS revenue_gbp
        FROM `storm-pub-amazon-sales.daily_sales.daily_sales_kenp_agg`
        WHERE ASIN IN ({asin_list})
        GROUP BY ASIN, sale_month, Marketplace
    """).to_dataframe()
    print(f"     {len(kenp_df)} rows")

    # ──────────────────────────────────────────────
    # STEP 3: Join sales to series metadata
    # ──────────────────────────────────────────────
    print("Step 3/7: Joining sales to series info...")

    meta_cols = ['Series', 'book_number', 'Title', 'Genre', 'Genre_Subgenre', 'Cover_Author']

    ebook_eds = edition_map[edition_map['Format'] == 'Ebook'][['ASIN'] + meta_cols].drop_duplicates()
    pod_eds = edition_map[edition_map['Format'] == 'POD'][['ISBN'] + meta_cols].drop_duplicates()

    ebook_merged = ebook_df.merge(ebook_eds, on='ASIN', how='inner')
    paperback_merged = paperback_df.merge(pod_eds, on='ISBN', how='inner')
    kenp_merged = kenp_df.merge(ebook_eds, on='ASIN', how='inner')

    for df in [ebook_merged, paperback_merged, kenp_merged]:
        df['territory'] = df['Marketplace'].apply(lambda m: 'GB' if m == 'Amazon.co.uk' else 'US')


    # ── Diagnostic: check B1 KENP by territory ──
    print("\n  Diagnostic: Book 1 KENP pages by territory...")
    b1_kenp = kenp_merged[kenp_merged['book_number'] == 1].groupby(
        ['Series', 'territory']
    ).agg(kenp_pages=('kenp_pages', 'sum')).reset_index()
    print(b1_kenp.sort_values('kenp_pages').head(20).to_string())

    # Which series have NO Book 1 KENP at all in a territory?
    all_series = kenp_merged['Series'].unique()
    b1_kenp_series = kenp_merged[kenp_merged['book_number'] == 1].groupby(
        ['Series', 'territory']
    )['kenp_pages'].sum().reset_index()

    for territory in ['GB', 'US']:
        has_b1 = set(b1_kenp_series[b1_kenp_series['territory'] == territory]['Series'])
        has_any = set(kenp_merged[kenp_merged['territory'] == territory]['Series'])
        missing_b1 = has_any - has_b1
        if missing_b1:
            print(f"\n  {territory} — series with KENP on later books but NO Book 1 KENP:")
            for s in sorted(missing_b1):
                print(f"    {s}")

    seb = b1_kenp[b1_kenp['Series'].str.contains('Sebastian Clifford')]
    print(seb)

    seb = kenp_merged[kenp_merged['Series'].str.contains('Sebastian Clifford')]
    seb_by_book = seb[seb['territory'] == 'GB'].groupby('book_number')['kenp_pages'].sum()
    print(seb_by_book.sort_index())

    seb_kenp = kenp_merged[kenp_merged['Series'].str.contains('Sebastian Clifford') & (kenp_merged['territory'] == 'GB')]
    seb_agg = seb_kenp.groupby(['Series', 'book_number', 'Title'])['kenp_pages'].sum().reset_index()
    print(seb_agg.sort_values('book_number').to_string())

    # ──────────────────────────────────────────────
    # STEP 4-7: Calculate per territory, per channel
    # ──────────────────────────────────────────────

    def calculate_for_territory(eb, pb, kn):
        """
        Returns a dict with three channel sub-dicts plus an overall summary.

        Channels:
          - alc_ebook:  readthrough by units, value = £ per Book 1 ALC sale
          - kenp:       readthrough by KENP pages, value = £ per Book 1 KENP page
          - pod:        readthrough by units, value = £ per Book 1 POD sale

        Overall:
          - roi_multiplier = total £ all channels / Book 1 £ all channels
        """
        grp = meta_cols

        # ── Lifetime aggregations per book ──
        ebook_life = eb.groupby(grp).agg(
            ebook_units=('units', 'sum'),
            ebook_revenue_gbp=('revenue_gbp', 'sum')
        ).reset_index()

        pb_life = pb.groupby(grp).agg(
            pod_units=('units', 'sum'),
            pod_revenue_gbp=('revenue_gbp', 'sum')
        ).reset_index()

        kenp_life = kn.groupby(grp).agg(
            kenp_pages=('kenp_pages', 'sum'),
            kenp_revenue_gbp=('revenue_gbp', 'sum')
        ).reset_index()

        meta_lookup = {}
        for src in [ebook_life, pb_life, kenp_life]:
            cols = ['Series', 'Genre', 'Genre_Subgenre', 'Cover_Author']
            available = [c for c in cols if c in src.columns]
            if len(available) == len(cols):
                for _, row in src[available].drop_duplicates('Series').iterrows():
                    if row['Series'] not in meta_lookup:
                        meta_lookup[row['Series']] = {
                            'Genre': str(row.get('Genre', '')),
                            'Genre_Subgenre': str(row.get('Genre_Subgenre', '')),
                            'Cover_Author': str(row.get('Cover_Author', '')),
                        }

        # Merge all three for the combined view
        book_totals = ebook_life.merge(pb_life, on=grp, how='outer') \
                                .merge(kenp_life, on=grp, how='outer')
        numeric_cols = book_totals.select_dtypes(include='number').columns
        book_totals[numeric_cols] = book_totals[numeric_cols].fillna(0)
        book_totals['total_revenue_gbp'] = (
            book_totals['ebook_revenue_gbp']
            + book_totals['pod_revenue_gbp']
            + book_totals['kenp_revenue_gbp']
        )
        # Collapse duplicate book numbers (e.g. translations) for overall calc too
        book_totals = book_totals.groupby(['Series', 'book_number']).agg({
            'ebook_units': 'sum', 'ebook_revenue_gbp': 'sum',
            'pod_units': 'sum', 'pod_revenue_gbp': 'sum',
            'kenp_pages': 'sum', 'kenp_revenue_gbp': 'sum',
            'total_revenue_gbp': 'sum',
        }).reset_index().sort_values(['Series', 'book_number']).reset_index(drop=True)

        # ── Channel-specific readthrough calculations ──

        def calc_channel_readthrough(df, unit_col, revenue_col, channel_name, meta_lookup):
            """
            Calculate readthrough curve for a single channel.

            For ALC ebook and POD: readthrough is unit-based.
            For KENP: readthrough is KENP-page-based (unit_col = 'kenp_pages').

            Value metric:
              - ALC/POD: cumulative £ per Book 1 unit sold
              - KENP:    cumulative £ per Book 1 KENP page
            """
            results = []
            # Collapse duplicate book numbers (e.g. translations sharing same series)
            df = df.groupby(['Series', 'book_number']).agg({
                unit_col: 'sum',
                revenue_col: 'sum'
            }).reset_index()
            for series_name, group in df.groupby('Series'):
                group = group.sort_values('book_number').reset_index(drop=True)
                if len(group) < 2:
                    continue
                b1 = group[group['book_number'] == 1]
                if b1.empty:
                    continue

                b1_base = b1[unit_col].sum()
                if b1_base <= 0:
                    continue

                group = group.copy()
                # Readthrough from Book 1 (based on channel's own unit)
                group['readthrough_from_book1'] = (group[unit_col] / b1_base).round(4)

                # Sequential readthrough (book N / book N-1)
                group['sequential_readthrough'] = (
                    group[unit_col] / group[unit_col].shift(1)
                ).round(4)
                group.loc[group.index[0], 'sequential_readthrough'] = None
                group['sequential_readthrough'] = group['sequential_readthrough'].replace([float('inf'), -float('inf')], None)


                # Value per Book 1 base unit
                # marginal = (revenue of this book) / b1_base
                group['marginal_value_per_b1_unit'] = (
                    group[revenue_col] / b1_base
                ).round(6)
                group['cumulative_value_per_b1_unit'] = (
                    group['marginal_value_per_b1_unit'].cumsum().round(6)
                )

                results.append(group)

            if not results:
                return pd.DataFrame(), {}, [], {}

            readthrough = pd.concat(results, ignore_index=True)

            # ── Build series summary rows ──
            summary_rows = []
            curves = {}
            monthly_data = {}

            for series_name, g in readthrough.groupby('Series'):
                g = g.sort_values('book_number')
                b1_group = g[g['book_number'] == 1]
                if b1_group.empty:
                    continue

                b1_base = b1_group[unit_col].sum()
                b1_rev = b1_group[revenue_col].sum()
                b1 = b1_group.iloc[0]
                total_rev = g[revenue_col].sum()

                seq = g[g['book_number'] > 1]['sequential_readthrough'].dropna()

                summary_rows.append({
                    'series': series_name,
                    'genre': meta_lookup.get(series_name, {}).get('Genre', ''),
                    'genre_subgenre': meta_lookup.get(series_name, {}).get('Genre_Subgenre', ''),
                    'author': meta_lookup.get(series_name, {}).get('Cover_Author', ''),
                    'total_books': int(g['book_number'].nunique()),
                    'book1_base': round(float(b1_base), 2),
                    'book1_revenue_gbp': round(float(b1_rev), 2),
                    'total_channel_revenue_gbp': round(float(total_rev), 2),
                    'channel_roi_multiplier': round(float(total_rev / b1_rev), 2) if b1_rev > 0 else 0,
                    'value_per_b1_unit': round(float(total_rev / b1_base), 6) if b1_base > 0 else 0,
                    'avg_readthrough': round(float(seq.mean()), 4) if len(seq) > 0 else 0,
                    'worst_readthrough': round(float(seq.min()), 4) if len(seq) > 0 else 0,
                })

                # Readthrough curve detail
                curve_cols = [
                    'book_number', 'Title', unit_col, revenue_col,
                    'readthrough_from_book1', 'sequential_readthrough',
                    'marginal_value_per_b1_unit', 'cumulative_value_per_b1_unit'
                ]
                curves[series_name] = g[[c for c in curve_cols if c in g.columns]].to_dict('records')

            summary_df = pd.DataFrame(summary_rows).sort_values('channel_roi_multiplier', ascending=False) if summary_rows else pd.DataFrame()

            # ── Monthly revenue per series for this channel ──
            if channel_name == 'alc_ebook':
                src = eb
                rev_col_monthly = 'revenue_gbp'
            elif channel_name == 'kenp':
                src = kn
                rev_col_monthly = 'revenue_gbp'
            else:
                src = pb
                rev_col_monthly = 'revenue_gbp'

            if len(src) > 0:
                monthly_agg = src.groupby(['Series', 'sale_month']).agg(
                    revenue_gbp=(rev_col_monthly, 'sum')
                ).reset_index().sort_values(['Series', 'sale_month'])

                if channel_name == 'kenp':
                    kenp_monthly_pages = kn.groupby(['Series', 'sale_month']).agg(
                        kenp_pages=('kenp_pages', 'sum')
                    ).reset_index()
                    monthly_agg = monthly_agg.merge(kenp_monthly_pages, on=['Series', 'sale_month'], how='left')

                for s, mg in monthly_agg.groupby('Series'):
                    monthly_data[s] = mg.drop(columns=['Series']).to_dict('records')

            # ── Genre stats ──
            genre_stats = []
            if len(summary_df) > 0:
                genre_stats = summary_df.groupby('genre').agg(
                    series_count=('series', 'count'),
                    avg_roi=('channel_roi_multiplier', 'mean'),
                    avg_readthrough=('avg_readthrough', 'mean'),
                    total_revenue=('total_channel_revenue_gbp', 'sum')
                ).reset_index().sort_values('avg_roi', ascending=False).to_dict('records')

            return {
                'series_roi': summary_df.to_dict('records') if len(summary_df) > 0 else [],
                'readthrough_curves': curves,
                'monthly_revenue': monthly_data,
                'genre_stats': genre_stats,
            }

        # ── Run each channel ──
        print("     ALC Ebook channel...")
        alc_result = calc_channel_readthrough(
            ebook_life, 'ebook_units', 'ebook_revenue_gbp', 'alc_ebook', meta_lookup
        )

        print("     KENP channel...")
        kenp_result = calc_channel_readthrough(
            kenp_life, 'kenp_pages', 'kenp_revenue_gbp', 'kenp', meta_lookup
        )

        print("     POD channel...")
        pod_result = calc_channel_readthrough(
            pb_life, 'pod_units', 'pod_revenue_gbp', 'pod', meta_lookup
        )

        # ── Overall summary (all channels combined, revenue-based ROI) ──
        overall_rows = []
        for series_name, g in book_totals.groupby('Series'):
            g = g.sort_values('book_number')
            if len(g) < 2:
                continue
            b1_group = g[g['book_number'] == 1]
            if b1_group.empty:
                continue

            b1_rev = (
                b1_group['ebook_revenue_gbp'].sum()
                + b1_group['pod_revenue_gbp'].sum()
                + b1_group['kenp_revenue_gbp'].sum()
            )
            b1 = b1_group.iloc[0]
            total_rev = g['total_revenue_gbp'].sum()

            if b1_rev <= 0:
                continue

            overall_rows.append({
                'series': series_name,
                'genre': str(b1.get('Genre', '')),
                'genre_subgenre': str(b1.get('Genre_Subgenre', '')),
                'author': str(b1.get('Cover_Author', '')),
                'total_books': int(g['book_number'].nunique()),
                'total_series_revenue_gbp': round(float(total_rev), 2),
                'book1_revenue_gbp': round(float(b1_rev), 2),
                'roi_multiplier': round(float(total_rev / b1_rev), 2),
                'ebook_revenue_gbp': round(float(g['ebook_revenue_gbp'].sum()), 2),
                'kenp_revenue_gbp': round(float(g['kenp_revenue_gbp'].sum()), 2),
                'pod_revenue_gbp': round(float(g['pod_revenue_gbp'].sum()), 2),
                'ebook_share': round(float(g['ebook_revenue_gbp'].sum() / total_rev), 4) if total_rev > 0 else 0,
                'kenp_share': round(float(g['kenp_revenue_gbp'].sum() / total_rev), 4) if total_rev > 0 else 0,
                'pod_share': round(float(g['pod_revenue_gbp'].sum() / total_rev), 4) if total_rev > 0 else 0,
            })

        overall_df = pd.DataFrame(overall_rows).sort_values('roi_multiplier', ascending=False) if overall_rows else pd.DataFrame()

        # ── Combined monthly revenue (all channels) ──
        ebook_m = eb.groupby(['Series', 'sale_month']).agg(
            ebook_revenue_gbp=('revenue_gbp', 'sum')).reset_index()
        pb_m = pb.groupby(['Series', 'sale_month']).agg(
            pod_revenue_gbp=('revenue_gbp', 'sum')).reset_index()
        kenp_m = kn.groupby(['Series', 'sale_month']).agg(
            kenp_revenue_gbp=('revenue_gbp', 'sum')).reset_index()

        monthly_all = ebook_m.merge(pb_m, on=['Series', 'sale_month'], how='outer') \
                             .merge(kenp_m, on=['Series', 'sale_month'], how='outer')
        num_cols = monthly_all.select_dtypes(include='number').columns
        monthly_all[num_cols] = monthly_all[num_cols].fillna(0)
        monthly_all['total_revenue_gbp'] = (
            monthly_all['ebook_revenue_gbp']
            + monthly_all['pod_revenue_gbp']
            + monthly_all['kenp_revenue_gbp']
        )
        monthly_all = monthly_all.sort_values(['Series', 'sale_month'])

        overall_monthly = {}
        for s, mg in monthly_all.groupby('Series'):
            overall_monthly[s] = mg[
                ['sale_month', 'total_revenue_gbp',
                 'ebook_revenue_gbp', 'pod_revenue_gbp', 'kenp_revenue_gbp']
            ].to_dict('records')

        # ── Summary stats ──
        def channel_summary(result):
            roi_list = result.get('series_roi', [])
            if not roi_list:
                return {'total_series': 0, 'avg_roi': 0, 'avg_readthrough': 0, 'total_revenue': 0}
            df = pd.DataFrame(roi_list)
            return {
                'total_series': int(len(df)),
                'avg_roi': round(float(df['channel_roi_multiplier'].mean()), 2),
                'avg_readthrough': round(float(df['avg_readthrough'].mean()), 4),
                'total_revenue': round(float(df['total_channel_revenue_gbp'].sum()), 2),
            }

        overall_summary = {
            'total_series': int(len(overall_df)),
            'avg_roi_multiplier': round(float(overall_df['roi_multiplier'].mean()), 2) if len(overall_df) > 0 else 0,
            'median_roi_multiplier': round(float(overall_df['roi_multiplier'].median()), 2) if len(overall_df) > 0 else 0,
            'total_revenue_gbp': round(float(overall_df['total_series_revenue_gbp'].sum()), 2) if len(overall_df) > 0 else 0,
        }

        return {
            'summary': overall_summary,
            'overall': {
                'series_roi': overall_df.to_dict('records') if len(overall_df) > 0 else [],
                'monthly_revenue': overall_monthly,
            },
            'alc_ebook': {
                'summary': channel_summary(alc_result),
                **alc_result,
            },
            'kenp': {
                'summary': channel_summary(kenp_result),
                **kenp_result,
            },
            'pod': {
                'summary': channel_summary(pod_result),
                **pod_result,
            },
        }

    # ──────────────────────────────────────────────
    # Run for each territory
    # ──────────────────────────────────────────────
    output = {'generated_at': datetime.now().isoformat()}

    for territory in ['Total', 'GB', 'US']:
        print(f"\n  Calculating for {territory}...")
        if territory == 'Total':
            eb, pb, kn = ebook_merged, paperback_merged, kenp_merged
        else:
            eb = ebook_merged[ebook_merged['territory'] == territory]
            pb = paperback_merged[paperback_merged['territory'] == territory]
            kn = kenp_merged[kenp_merged['territory'] == territory]
        output[territory] = calculate_for_territory(eb, pb, kn)

    # Clean ALL bad values
    output = clean_data(output)

    # Save JSON (for hosted usage)
    with open('series_projection_data.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    # Save JS (for local file:// usage)
    with open('series_projection_data.js', 'w', encoding='utf-8') as f:
        f.write('var DATA = ')
        json.dump(output, f, indent=2)
        f.write(';')

    total = output['Total']
    print(f"\n{'='*50}")
    print(f"  DONE — series_projection_data.json saved")
    print(f"  {total['summary']['total_series']} series analysed")
    print(f"  Overall avg ROI: {total['summary']['avg_roi_multiplier']}x")
    print(f"  ALC Ebook — {total['alc_ebook']['summary']['total_series']} series, avg ROI {total['alc_ebook']['summary']['avg_roi']}x")
    print(f"  KENP      — {total['kenp']['summary']['total_series']} series, avg ROI {total['kenp']['summary']['avg_roi']}x")
    print(f"  POD       — {total['pod']['summary']['total_series']} series, avg ROI {total['pod']['summary']['avg_roi']}x")
    print(f"{'='*50}")



    # ── Upload standalone dashboard to Cloud Storage ──
    from google.cloud import storage as gcs

    print("\n  Building standalone HTML...")
    with open('series.html', 'r', encoding='utf-8') as f:
        html = f.read()

    json_str = json.dumps(output, indent=2)
    old_tag = '<script src="series_projection_data.js" onerror="console.log(\'No .js file found, will try fetch\')"></script>'
    new_tag = '<script>var DATA = ' + json_str + ';</script>'
    html = html.replace(old_tag, new_tag)

    with open('index.html', 'w', encoding='utf-8') as f:
        f.write(html)

    print("  Uploading to gs://storm-series-dashboard/index.html...")
    gcs_client = gcs.Client()
    bucket = gcs_client.bucket('storm-series-dashboard')
    blob = bucket.blob('index.html')
    blob.content_type = 'text/html'
    blob.upload_from_filename('index.html')
    print("  Done! Dashboard live at https://storage.googleapis.com/storm-series-dashboard/index.html")

    return output


if __name__ == '__main__':
    run_pipeline()