# generate_genre_analysis.py
"""
Genre-level analysis for active campaigns.
For each running book: shows its metrics vs genre/sub-genre benchmarks,
compares GB vs US, and suggests budget actions.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import os

try:
    from bq import get_client
except ImportError:
    get_client = None

OUTPUT_DIR = os.getenv("GENRE_OUTPUT_DIR", "genre_reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

C = {
    "bg": "#FEFCF8",
    "card": "#FFFFFF",
    "header_bg": "#1B1B2F",
    "accent": "#E94560",
    "accent2": "#0984E3",
    "text_dark": "#1B1B2F",
    "text_mid": "#555555",
    "text_light": "#999999",
    "positive": "#00B894",
    "negative": "#E94560",
    "neutral": "#F4C430",
    "divider": "#E0DCD5",
    "row_alt": "#F7F5F0",
    "gb": "#E94560",
    "us": "#0984E3",
}

GENRE_COLORS = {
    "Crime thriller": "#E94560",
    "Historical": "#8B5CF6",
    "Women's fiction": "#F59E0B",
    "Chicklit": "#EC4899",
    "Romance": "#EF4444",
    "SFF": "#06B6D4",
}


def fetch_data(date=None):
    """Fetch active campaigns + genre benchmarks."""
    client = get_client()
    date_filter = f"DATE('{date}')" if date else """
    (
    SELECT MAX(date_start)
    FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    )
    """

    # Yesterday's active campaigns per book per territory
    active_query = f"""
    SELECT
        Title, Edition_ID, Genre, Genre_Subgenre, Territory,
        SUM(spend) AS spend, SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(ebook_units) AS ebook_units,
        SUM(paperback_units) AS paperback_units,
        SUM(kenp) AS kenp,
        SUM(ebook_revenue) AS ebook_revenue,
        SUM(paperback_revenue) AS paperback_revenue,
        SUM(kenp_revenue) AS kenp_revenue
    FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    WHERE date_start = {date_filter}
        AND Genre IS NOT NULL AND Genre != 'nan'
    GROUP BY Title, Edition_ID, Genre, Genre_Subgenre, Territory
    """

    # Current run totals per book per territory (using gap detection)
    run_query = f"""
    WITH run_dates AS (
        SELECT Edition_ID, Territory, date_start,
            DATE_DIFF(date_start, LAG(date_start) OVER (
                PARTITION BY Edition_ID, Territory ORDER BY date_start
            ), DAY) AS gap
        FROM `marketing-489109.facebook_ads.ads_sales_analytics`
        WHERE date_start <= {date_filter}
    ),
    run_starts AS (
        SELECT Edition_ID, Territory, MAX(date_start) AS run_start
        FROM run_dates WHERE gap IS NULL OR gap > 1
        GROUP BY Edition_ID, Territory
    )
    SELECT
        a.Title, a.Edition_ID, a.Genre, a.Genre_Subgenre, a.Territory,
        COUNT(DISTINCT a.date_start) AS run_days,
        SUM(a.spend) AS spend, SUM(a.clicks) AS clicks,
        SUM(a.impressions) AS impressions,
        SUM(a.ebook_units) AS ebook_units,
        SUM(a.paperback_units) AS paperback_units,
        SUM(a.kenp) AS kenp,
        SUM(a.ebook_revenue) AS ebook_revenue,
        SUM(a.paperback_revenue) AS paperback_revenue,
        SUM(a.kenp_revenue) AS kenp_revenue
    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a
    JOIN run_starts r ON a.Edition_ID = r.Edition_ID
        AND a.Territory = r.Territory
        AND a.date_start >= r.run_start
    WHERE a.date_start <= {date_filter}
        AND a.Genre IS NOT NULL AND a.Genre != 'nan'
    GROUP BY a.Title, a.Edition_ID, a.Genre, a.Genre_Subgenre, a.Territory
    """

    # Genre benchmarks (historical daily averages)
    benchmark_query = f"""
    SELECT
        Genre, Genre_Subgenre, Territory,
        COUNT(DISTINCT Edition_ID) AS total_books,
        AVG(spend) AS avg_spend, AVG(clicks) AS avg_clicks,
        AVG(impressions) AS avg_impressions,
        AVG(SAFE_DIVIDE(spend, NULLIF(clicks, 0))) AS avg_cpc,
        AVG(SAFE_DIVIDE(clicks, NULLIF(impressions, 0)) * 100) AS avg_ctr,
        AVG(kenp_revenue) AS avg_kenp_revenue,
        AVG(ebook_units) AS avg_ebook_units,
        AVG(kenp) AS avg_kenp,
        AVG(ebook_revenue) AS avg_ebook_revenue,
        AVG(paperback_revenue) AS avg_paperback_revenue,
        AVG(ebook_revenue + paperback_revenue + kenp_revenue) AS avg_revenue,
        AVG(SAFE_DIVIDE((ebook_revenue + paperback_revenue + kenp_revenue) * 0.5 - spend, NULLIF(spend, 0)) * 100) AS avg_roi
    FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    WHERE Genre IS NOT NULL AND Genre != 'nan'
    GROUP BY Genre, Genre_Subgenre, Territory
    """

    active_df = client.query(active_query).to_dataframe()
    run_df = client.query(run_query).to_dataframe()
    bench_df = client.query(benchmark_query).to_dataframe()

    return active_df, run_df, bench_df


def calc(df):
    """Calculate derived metrics from a row or aggregated data."""
    spend = df["spend"] if isinstance(df, dict) else df["spend"].sum()
    clicks = df["clicks"] if isinstance(df, dict) else df["clicks"].sum()
    impressions = df["impressions"] if isinstance(df, dict) else df["impressions"].sum()
    ebook_units = df["ebook_units"] if isinstance(df, dict) else df["ebook_units"].sum()
    pb_units = df["paperback_units"] if isinstance(df, dict) else df["paperback_units"].sum()
    kenp = df["kenp"] if isinstance(df, dict) else df["kenp"].sum()
    ebook_rev = df["ebook_revenue"] if isinstance(df, dict) else df["ebook_revenue"].sum()
    pb_rev = df["paperback_revenue"] if isinstance(df, dict) else df["paperback_revenue"].sum()
    kenp_rev = df["kenp_revenue"] if isinstance(df, dict) else df["kenp_revenue"].sum()

    revenue = ebook_rev + pb_rev + kenp_rev
    pub_rev = revenue * 0.5

    return {
        "spend": spend,
        "clicks": clicks,
        "impressions": impressions,
        "cpc": spend / clicks if clicks > 0 else 0,
        "ctr": (clicks / impressions * 100) if impressions > 0 else 0,
        "ebook_units": ebook_units,
        "pb_units": pb_units,
        "kenp": kenp,

        # IMPORTANT: include these so benchmark section works
        "ebook_revenue": ebook_rev,
        "paperback_revenue": pb_rev,
        "kenp_revenue": kenp_rev,

        "revenue": revenue,
        "pub_rev": pub_rev,
        "profit": pub_rev - spend,
        "ad_pct": (spend / revenue * 100) if revenue > 0 else 0,
        "roi": ((pub_rev - spend) / spend * 100) if spend > 0 else 0,
    }

def fc(val, s="£"):
    if abs(val) >= 1000:
        return f"{s}{val:,.0f}"
    return f"{s}{val:,.2f}"


def fn(val):
    return f"{int(val):,}" if val == int(val) else f"{val:,.1f}"


def fp0(val):
    return f"{val:.0f}%"

def fp(val):
    return f"{val:.1f}%"


def signal(book_val, bench_val, lower_is_better=False):
    """Return symbol comparing book vs benchmark."""
    if bench_val == 0:
        return "--"
    diff = ((book_val - bench_val) / bench_val) * 100
    if lower_is_better:
        diff = -diff
    if diff > 15:
        return "[+]"
    elif diff < -15:
        return "[-]"
    return "[=]"


def generate_book_genre_card(title, edition_id, genre, subgenre,
                              yesterday, run_totals, benchmarks):
    """Generate a genre analysis card for one book."""

    territories = sorted(set(
        [t for t in yesterday.keys()] + [t for t in run_totals.keys()]
    ))

    if not territories:
        return None

    n_terr = len(territories)
    
    # Layout
    row_h = 0.38
    header_h = 1.6
    metrics_section = 10  # metric rows
    bench_section = 10  # benchmark comparison rows
    total_rows = metrics_section + bench_section + 4  # +4 for section headers + recommendation
    fig_h = header_h + (total_rows * row_h) + 1.5
    terr_w = 4.8
    label_w = 3.2
    fig_w = label_w + (n_terr * terr_w) + 0.6

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor(C["bg"])
    ax.set_facecolor(C["bg"])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.axis("off")

    # ── Header ──
    genre_color = GENRE_COLORS.get(genre, C["accent"])
    header = mpatches.FancyBboxPatch(
        (0.2, fig_h - header_h), fig_w - 0.4, header_h - 0.1,
        boxstyle="round,pad=0.1",
        facecolor=C["header_bg"], edgecolor=genre_color, linewidth=2.5,
    )
    ax.add_patch(header)

    # Genre badge
    badge = mpatches.FancyBboxPatch(
        (0.5, fig_h - 0.5), len(genre) * 0.12 + 0.4, 0.35,
        boxstyle="round,pad=0.05",
        facecolor=genre_color, edgecolor="none",
    )
    ax.add_patch(badge)
    ax.text(0.5 + (len(genre) * 0.12 + 0.4) / 2, fig_h - 0.33,
            genre, fontsize=9, fontweight="bold", color="white",
            ha="center", va="center", fontfamily="sans-serif")

    # Title
    display_title = title if len(title) < 45 else title[:42] + "..."
    ax.text(0.5, fig_h - 0.85, display_title,
            fontsize=18, fontweight="bold", color="white", fontfamily="sans-serif")

    # Sub-genre + Edition
    sub_display = subgenre if subgenre and str(subgenre) != "nan" else "—"
    ax.text(0.5, fig_h - 1.2,
            f"Sub-genre: {sub_display}  •  Edition {edition_id}",
            fontsize=9, color="#AAAAAA", fontfamily="sans-serif")

    # Territory headers
    y_start = fig_h - header_h - 0.2
    for i, terr in enumerate(territories):
        tx = label_w + (i * terr_w) + (terr_w / 2)
        color = C["gb"] if terr == "GB" else C["us"]

        # Territory pill — split Yesterday | Run Total
        for j, (lbl, sub_color) in enumerate([("Yesterday", color), ("Campaign Total", "#555555")]):
            sub_x = label_w + (i * terr_w) + (j * terr_w / 2) + (terr_w / 4)
            pill = mpatches.FancyBboxPatch(
                (sub_x - 0.9, y_start - 0.32), 1.8, 0.32,
                boxstyle="round,pad=0.04",
                facecolor=sub_color + "22", edgecolor=sub_color, linewidth=1,
            )
            ax.add_patch(pill)
            ax.text(sub_x, y_start - 0.16, lbl,
                    fontsize=7, fontweight="bold", color=sub_color,
                    ha="center", va="center", fontfamily="sans-serif")

        # Territory name above
        hdr = mpatches.FancyBboxPatch(
            (label_w + (i * terr_w) + 0.1, y_start + 0.08),
            terr_w - 0.2, 0.35,
            boxstyle="round,pad=0.05",
            facecolor=color, edgecolor="none",
        )
        ax.add_patch(hdr)
        ax.text(tx, y_start + 0.25, terr,
                fontsize=11, fontweight="bold", color="white",
                ha="center", va="center", fontfamily="sans-serif")

    # ── SECTION 1: Campaign Metrics ──
    y = y_start - 0.65
    ax.text(0.45, y + 0.05, "CAMPAIGN METRICS",
            fontsize=9, fontweight="bold", color=genre_color,
            fontfamily="sans-serif")
    y -= row_h

    metric_rows = [
        ("Spend", "spend", fc, False),
        ("Clicks", "clicks", fn, False),
        ("CPC", "cpc", fc, True),
        ("CTR", "ctr", fp, False),
        ("Ebook Units", "ebook_units", fn, False),
        ("Paperback Units", "pb_units", fn, False),
        ("KENP", "kenp", fn, False),
        ("Revenue", "revenue", fc, False),
        ("Publisher Rev", "pub_rev", fc, False),
        ("Gross Profit", "profit", fc, False),
        ("ROI", "roi", fp0, False),
    ]

    for idx, (label, key, fmt, lower_better) in enumerate(metric_rows):
        row_color = C["row_alt"] if idx % 2 == 0 else C["card"]
        rect = mpatches.Rectangle(
            (0.2, y - 0.15), fig_w - 0.4, row_h,
            facecolor=row_color, edgecolor="none",
        )
        ax.add_patch(rect)

        ax.text(0.45, y + 0.04, label,
                fontsize=9, color=C["text_mid"], va="center", fontfamily="sans-serif")

        for i, terr in enumerate(territories):
            y_data = yesterday.get(terr, {})
            r_data = run_totals.get(terr, {})

            # Yesterday value
            y_val = y_data.get(key, 0)
            y_x = label_w + (i * terr_w) + (terr_w / 4)

            val_color = C["text_dark"]
            if key == "profit":
                val_color = C["positive"] if y_val >= 0 else C["negative"]
            elif key == "roi":
                val_color = C["positive"] if y_val > 0 else C["negative"]   
            ax.text(y_x, y + 0.04, fmt(y_val),
                    fontsize=10, fontweight="bold", color=val_color,
                    ha="center", va="center", fontfamily="sans-serif")

            # Run total value
            r_val = r_data.get(key, 0)
            r_x = label_w + (i * terr_w) + (3 * terr_w / 4)

            r_color = C["text_dark"]
            if key == "profit":
                r_color = C["positive"] if r_val >= 0 else C["negative"]
            elif key == "roi":
                r_color = C["positive"] if r_val > 0 else C["negative"]

            ax.text(r_x, y + 0.04, fmt(r_val),
                    fontsize=10, fontweight="bold", color=r_color,
                    ha="center", va="center", fontfamily="sans-serif")

        y -= row_h

    # ── SECTION 2: vs Genre Benchmark ──
    y -= 0.2
    ax.text(0.45, y + 0.05, "vs 12-MONTH GENRE BENCHMARK",
            fontsize=9, fontweight="bold", color=genre_color,
            fontfamily="sans-serif")
    y -= row_h

    bench_rows = [
        ("CPC", "cpc", "avg_cpc", fc, True),
        ("CTR", "ctr", "avg_ctr", fp, False),
        ("Daily ALC Ebook Revenue", "ebook_revenue", "avg_ebook_revenue", fc, False),
        ("Daily KENP Revenue", "kenp_revenue", "avg_kenp_revenue", fc, False),
        ("Daily Revenue", "revenue", "avg_revenue", fc, False),
        ("Daily Spend", "spend", "avg_spend", fc, True),
        ("ROI", "roi", "avg_roi", fp0, True)
    ]

    for idx, (label, book_key, bench_key, fmt, lower_better) in enumerate(bench_rows):
        row_color = C["row_alt"] if idx % 2 == 0 else C["card"]
        rect = mpatches.Rectangle(
            (0.2, y - 0.15), fig_w - 0.4, row_h,
            facecolor=row_color, edgecolor="none",
        )
        ax.add_patch(rect)

        ax.text(0.45, y + 0.04, label,
                fontsize=9, color=C["text_mid"], va="center", fontfamily="sans-serif")

        for i, terr in enumerate(territories):
            y_data = run_totals.get(terr, {})
            b_data = benchmarks.get(terr, {})

            # Skip benchmarks for territories with no active spend
            if not y_data or y_data.get("spend", 0) == 0:
                y_x = label_w + (i * terr_w) + (terr_w / 4)
                ax.text(y_x, y + 0.04, "—",
                        fontsize=10, color=C["text_light"],
                        ha="center", va="center", fontfamily="sans-serif")
                continue

            run_days = y_data.get("run_days", 1)

            book_val = y_data.get(book_key, 0)
            if book_key not in ["cpc", "ctr", "roi"] and run_days > 0:
                book_val = book_val / run_days
            bench_val = b_data.get(bench_key, 0)
            sig = signal(book_val, bench_val, lower_better)

            # Book value vs benchmark
            y_x = label_w + (i * terr_w) + (terr_w / 4)
            r_x = label_w + (i * terr_w) + (3 * terr_w / 4)

            # Colored dot indicator
            dot_color = C["positive"] if sig == "[+]" else C["negative"] if sig == "[-]" else C["text_light"]
            dot = mpatches.Circle((y_x - 0.8, y + 0.04), 0.08, facecolor=dot_color, edgecolor="none")
            ax.add_patch(dot)

            ax.text(y_x, y + 0.04, fmt(book_val),
                    fontsize=10, fontweight="bold", color=C["text_dark"],
                    ha="center", va="center", fontfamily="sans-serif")
            ax.text(r_x, y + 0.04, f"avg: {fmt(bench_val)}",
                    fontsize=9, color=C["text_light"],
                    ha="center", va="center", fontfamily="sans-serif")

        y -= row_h

    # ── SECTION 3: Recommendation ──
    y -= 0.3
    rec_rect = mpatches.FancyBboxPatch(
        (0.3, y - 0.7), fig_w - 0.6, 1.0,
        boxstyle="round,pad=0.08",
        facecolor=genre_color + "15", edgecolor=genre_color, linewidth=1.5,
    )
    ax.add_patch(rec_rect)

    ax.text(0.5, y - 0.05, "RECOMMENDATION",
            fontsize=9, fontweight="bold", color=genre_color, fontfamily="sans-serif")

    # Generate recommendation
    recs = []

    active_territories = [t for t in territories if yesterday.get(t, {}).get("spend", 0) > 0]
    if len(active_territories) == 2:
        gb_y = yesterday.get("GB", {})
        us_y = yesterday.get("US", {})
        gb_profit = gb_y.get("profit", 0)
        us_profit = us_y.get("profit", 0)
        gb_ad_pct = gb_y.get("ad_pct", 999)
        us_ad_pct = us_y.get("ad_pct", 999)

        if gb_profit > 0 and us_profit < 0:
            recs.append("GB is profitable, US is losing money → Consider shifting US budget to GB")
        elif us_profit > 0 and gb_profit < 0:
            recs.append("US is profitable, GB is losing money → Consider shifting GB budget to US")
        elif gb_profit < 0 and us_profit < 0:
            if gb_ad_pct < us_ad_pct:
                recs.append(f"Both territories losing money. GB ({fp(gb_ad_pct)} ad/rev) less bad than US ({fp(us_ad_pct)}) → Prioritise GB or reduce both")
            else:
                recs.append(f"Both territories losing money. US ({fp(us_ad_pct)} ad/rev) less bad than GB ({fp(gb_ad_pct)}) → Prioritise US or reduce both")
        else:
            recs.append("Both territories profitable! Consider increasing budget to scale")

        # CPC comparison vs benchmark
        for terr in territories:
            y_data = yesterday.get(terr, {})
            b_data = benchmarks.get(terr, {})
            if y_data.get("cpc", 0) > b_data.get("avg_cpc", 0) * 1.3:
                recs.append(f"{terr} CPC is 30%+ above genre average → Review targeting")

    elif len(active_territories) == 1:
        terr = active_territories[0]
        y_data = yesterday.get(terr, {})
        if y_data.get("profit", 0) > 0:
            recs.append(f"Profitable in {terr}! Consider expanding to {'US' if terr == 'GB' else 'GB'}")
        else:
            recs.append(f"Losing money in {terr} → Review spend or pause if trend continues")

    if not recs:
        recs.append("Monitor performance — insufficient data for strong recommendation")

    rec_text = " | ".join(recs[:2])
    if len(rec_text) > 200:
        rec_text = rec_text[:197] + "..."
    ax.text(fig_w / 2, y - 0.35, rec_text,
            fontsize=8, color=C["text_dark"],
            ha="center", va="center", fontfamily="sans-serif",
            wrap=True)

    # ── Bottom line ──
    ax.plot([0.3, fig_w - 0.3], [0.15, 0.15], color=genre_color, linewidth=2)

    plt.tight_layout(pad=0.3)

    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in title).strip()
    safe_title = safe_title.replace(" ", "_")[:40]
    filename = f"{OUTPUT_DIR}/{safe_title}_{edition_id}.png"
    fig.savefig(filename, dpi=150, bbox_inches="tight", facecolor=C["bg"])
    plt.close(fig)

    print(f"  ✓ {filename}")
    return filename


def generate_all(date=None):
    """Generate genre analysis for all active books."""
    print("Fetching data...")
    active_df, run_df, bench_df = fetch_data(date)

    if active_df.empty:
        print("No active campaigns found.")
        return []

    books = active_df[["Title", "Edition_ID", "Genre", "Genre_Subgenre"]].drop_duplicates()
    print(f"Found {len(books)} active books\n")

    filenames = []
    for _, row in books.iterrows():
        title = row["Title"]
        eid = row["Edition_ID"]
        genre = row["Genre"]
        subgenre = row["Genre_Subgenre"]

        # Yesterday metrics per territory
        yesterday = {}
        for terr in active_df[active_df["Edition_ID"] == eid]["Territory"].unique():
            t_data = active_df[(active_df["Edition_ID"] == eid) & (active_df["Territory"] == terr)]
            yesterday[terr] = calc(t_data)

        # Run totals per territory

        run_totals = {}
        for terr in run_df[run_df["Edition_ID"] == eid]["Territory"].unique():
            if terr not in yesterday:
                continue
            t_data = run_df[(run_df["Edition_ID"] == eid) & (run_df["Territory"] == terr)]
            m = calc(t_data)
            days = t_data["run_days"].iloc[0] if not t_data.empty else 1
            m["run_days"] = days
            run_totals[terr] = m

        # Benchmarks for this genre + sub-genre
        benchmarks = {}
        for terr in ["GB", "US"]:
            b = bench_df[
                (bench_df["Genre_Subgenre"] == subgenre) &
                (bench_df["Territory"] == terr)
            ]
            if b.empty:
                b = bench_df[
                    (bench_df["Genre"] == genre) &
                    (bench_df["Territory"] == terr)
                ]
            if not b.empty:
                benchmarks[terr] = b.iloc[0].to_dict()
            else:
                benchmarks[terr] = {}

        print(f"📖 {title} ({genre})")
        fname = generate_book_genre_card(
            title, eid, genre, subgenre,
            yesterday, run_totals, benchmarks
        )
        if fname:
            filenames.append(fname)

    print(f"\nDone! {len(filenames)} reports in '{OUTPUT_DIR}/'")
    return filenames


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else None
    generate_all(date)