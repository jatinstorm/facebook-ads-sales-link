# launch_comparison.py
"""
Book launch scorecard — simplified for Slack.
Generates one card per milestone (30d, 90d, 12m).
Compares this book's TOTAL sales (all territories) vs catalogue average.

Metrics: Revenue, Units Sold, KENP reads.
Score: 0–100 composite from percentile ranks across all 3 metrics.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import os
from datetime import date, timedelta

try:
    from bq import get_client
except ImportError:
    get_client = None

OUTPUT_DIR = os.getenv("LAUNCH_OUTPUT_DIR", "launch_reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Colours ────────────────────────────────────────────────────
GREEN, GREEN_BG = "#1D9E75", "#E1F5EE"
AMBER, AMBER_BG = "#BA7517", "#FAEEDA"
RED, RED_BG = "#E24B4A", "#FCEBEB"

C = {
    "bg": "#F8F7F4",
    "card": "#FFFFFF",
    "header_bg": "#1B1B2F",
    "accent": "#E94560",
    "text_dark": "#2C2C2A",
    "text_mid": "#555555",
    "text_light": "#73726c",
    "grey_light": "#A0A09A",
    "bar_track": "#EEEEE8",
    "positive": GREEN,
    "negative": RED,
}

PERIOD_CONFIG = {
    "30d": {"label": "30 days", "days": 30, "color": "#E94560"},
    "90d": {"label": "90 days", "days": 90, "color": "#0984E3"},
    "12m": {"label": "12 months", "days": 365, "color": "#8B5CF6"},
}


# ═══════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_daily_launch_data(days=30):
    """
    Fetch first N days of sales per book.
    Only includes books that have REACHED N days since pub_date,
    so partial-data books don't drag down averages.
    """
    client = get_client()

    query = f"""
    WITH book_info AS (
        SELECT
            e.ID AS edition_id,
            e.Title AS title,
            e.Cover_Author AS author,
            e.Pub_Date AS pub_date,
            e.Genre AS genre,
            e.Genre_Subgenre AS genre_subgenre,
            FORMAT_DATE('%Y-%m', e.Pub_Date) AS pub_month,
            MIN(eb.ASIN) AS asin,
            CAST(MIN(p.ISBN) AS STRING) AS isbn
        FROM `storm-pub-amazon-sales.airtable.awe_editions` e
        JOIN `storm-pub-amazon-sales.airtable.awe_editions` eb
            ON e.Title = eb.Title AND eb.Format = 'Ebook'
        LEFT JOIN `storm-pub-amazon-sales.airtable.awe_editions` p
            ON e.Title = p.Title AND p.Format = 'POD'
        WHERE e.Format = 'Ebook'
            AND e.Pub_Date IS NOT NULL
            AND DATE_DIFF(CURRENT_DATE(), e.Pub_Date, DAY) >= {days}
        GROUP BY e.ID, e.Title, e.Cover_Author, e.Pub_Date, e.Genre, e.Genre_Subgenre
    ),
    ebook_sales AS (
        SELECT b.edition_id, b.title, b.author, b.pub_date, b.genre,
            b.genre_subgenre, b.pub_month,
            CASE WHEN s.Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS territory,
            SUM(IFNULL(s.Net_Units_Sold, 0)) AS ebook_units,
            SUM(IFNULL(s.Royalty_GBP, 0)) AS ebook_revenue
        FROM book_info b
        JOIN `storm-pub-amazon-sales.daily_sales.daily_sales_ebook_agg` s
            ON b.asin = s.ASIN
            AND s.Royalty_Date BETWEEN b.pub_date
                AND DATE_ADD(b.pub_date, INTERVAL {days - 1} DAY)
        GROUP BY b.edition_id, b.title, b.author, b.pub_date, b.genre,
                 b.genre_subgenre, b.pub_month, territory
    ),
    pb_sales AS (
        SELECT b.edition_id,
            CASE WHEN s.Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS territory,
            SUM(IFNULL(s.Net_Units_Sold, 0)) AS pb_units,
            SUM(IFNULL(s.Royalty_GBP, 0)) AS pb_revenue
        FROM book_info b
        JOIN `storm-pub-amazon-sales.daily_sales.daily_sales_paperback_agg` s
            ON b.isbn = CAST(s.ISBN AS STRING)
            AND s.Royalty_Date BETWEEN b.pub_date
                AND DATE_ADD(b.pub_date, INTERVAL {days - 1} DAY)
        WHERE b.isbn IS NOT NULL
        GROUP BY b.edition_id, territory
    )
    SELECT
        e.edition_id, e.title, e.author, e.pub_date, e.genre,
        e.genre_subgenre, e.pub_month, e.territory,
        e.ebook_units + IFNULL(p.pb_units, 0) AS units,
        e.ebook_revenue + IFNULL(p.pb_revenue, 0) AS revenue
    FROM ebook_sales e
    LEFT JOIN pb_sales p ON e.edition_id = p.edition_id AND e.territory = p.territory
    """

    return client.query(query).to_dataframe()


def fetch_daily_kenp_data(days=30):
    """
    Fetch first N days KENP. Only books that have reached N days.
    """
    client = get_client()

    query = f"""
    WITH book_info AS (
        SELECT
            e.ID AS edition_id,
            e.Pub_Date AS pub_date,
            MIN(eb.ASIN) AS asin
        FROM `storm-pub-amazon-sales.airtable.awe_editions` e
        JOIN `storm-pub-amazon-sales.airtable.awe_editions` eb
            ON e.Title = eb.Title AND eb.Format = 'Ebook'
        WHERE e.Format = 'Ebook'
            AND e.Pub_Date IS NOT NULL
            AND DATE_DIFF(CURRENT_DATE(), e.Pub_Date, DAY) >= {days}
        GROUP BY e.ID, e.Pub_Date
    )
    SELECT
        b.edition_id,
        CASE WHEN k.Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS territory,
        SUM(IFNULL(k.KENP, 0)) AS kenp,
        SUM(IFNULL(k.Royalty_GBP, 0)) AS kenp_revenue
    FROM book_info b
    JOIN `storm-pub-amazon-sales.daily_sales.daily_sales_kenp_agg` k
        ON b.asin = k.ASIN
        AND k.Date BETWEEN b.pub_date
            AND DATE_ADD(b.pub_date, INTERVAL {days - 1} DAY)
    GROUP BY b.edition_id, territory
    """

    return client.query(query).to_dataframe()


def fetch_monthly_launch_data():
    """Fetch first 12 months from monthly sales. Only books 12+ months old."""
    client = get_client()

    query = """
    SELECT
        e.ID AS edition_id,
        e.Title AS title,
        e.Cover_Author AS author,
        e.Pub_Date AS pub_date,
        e.Genre AS genre,
        e.Genre_Subgenre AS genre_subgenre,
        FORMAT_DATE('%Y-%m', e.Pub_Date) AS pub_month,
        CASE WHEN m.marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS territory,
        SUM(CASE WHEN m.payout_plan IN ('Standard', 'Pre Order')
            THEN IFNULL(m.net_units_sold, 0) ELSE 0 END) AS units,
        SUM(IFNULL(m.gbp_revenue, 0)) AS revenue,
        SUM(CASE WHEN m.payout_plan = 'Kindle Edition Normalized Pages (KENP)'
            THEN IFNULL(m.ku_books_read, 0) ELSE 0 END) AS kenp
    FROM `storm-pub-amazon-sales.airtable.awe_editions` e
    JOIN `storm-pub-amazon-sales.monthly_sales.monthly_sales` m
        ON e.ID = m.edition_id
        AND m.date BETWEEN DATE_TRUNC(e.Pub_Date, MONTH)
            AND DATE_ADD(DATE_TRUNC(e.Pub_Date, MONTH), INTERVAL 11 MONTH)
    WHERE e.Format = 'Ebook'
        AND e.Pub_Date IS NOT NULL
        AND DATE_DIFF(CURRENT_DATE(), e.Pub_Date, DAY) >= 365
    GROUP BY e.ID, e.Title, e.Cover_Author, e.Pub_Date, e.Genre,
             e.Genre_Subgenre, pub_month, territory
    """

    return client.query(query).to_dataframe()


# ═══════════════════════════════════════════════════════════════
# AGGREGATION & SCORING
# ═══════════════════════════════════════════════════════════════

def merge_kenp(daily_df, kenp_df):
    """Merge KENP data into daily sales dataframe."""
    if kenp_df.empty:
        daily_df["kenp"] = 0
        return daily_df

    merge_keys = ["edition_id"]
    if "territory" in kenp_df.columns and "territory" in daily_df.columns:
        merge_keys.append("territory")
    kenp_grouped = kenp_df.groupby(merge_keys).agg({"kenp": "sum"}).reset_index()
    merged = daily_df.merge(kenp_grouped, on=merge_keys, how="left")
    return merged


def build_totals(df):
    """
    Build per-book totals across ALL territories.
    ALSO keeps territory-level data for later use.
    """
    if df.empty:
        return df

    agg_cols = {"units": "sum", "revenue": "sum", "kenp": "sum"}
    meta_cols = ["title", "author", "pub_date", "genre", "genre_subgenre", "pub_month"]

    # ── GLOBAL totals (same as before) ──
    meta = df.groupby("edition_id")[meta_cols].first().reset_index()
    nums = df.groupby("edition_id")[list(agg_cols.keys())].sum().reset_index()
    global_df = meta.merge(nums, on="edition_id")

    # ── KEEP territory-level data (NEW) ──
    # We store it as a separate dataframe attached to global_df
    territory_df = df.copy()

    return {
        "global": global_df,
        "by_territory": territory_df
    }

def compute_percentile(book_val, all_values):
    """Compute the percentile rank of book_val within all_values."""
    if len(all_values) == 0:
        return 50
    rank = np.searchsorted(np.sort(all_values), book_val, side="right")
    return min(99, max(1, round(rank / len(all_values) * 100)))



def compute_percentiles_by_market(df, eid):
    """
    Compute percentile ranks separately for GB and US.
    df must be territory-level (not global).
    """
    results = {}

    for market in ["GB", "US"]:
        sub = df[df["territory"] == market]

        book_row = sub[sub["edition_id"] == eid]
        others = sub[sub["edition_id"] != eid]

        if book_row.empty or others.empty:
            continue

        results[market] = {
            key: compute_percentile(book_row[key].iloc[0], others[key].values)
            for key in ["units", "revenue", "kenp"]
        }

    return results


def build_scorecard(df_global, df_territory, eid):
    """
    Build scorecard data for one book from a totals dataframe.
    Returns dict with book metrics, catalogue/genre/author averages, and percentiles.
    """
    book_row = df_global[df_global["edition_id"] == eid]
    if book_row.empty:
        return None

    others = df_global[df_global["edition_id"] != eid]
    book_genre = book_row["genre"].iloc[0]
    book_author = book_row["author"].iloc[0]

    book = {
        "units": book_row["units"].iloc[0],
        "revenue": book_row["revenue"].iloc[0],
        "kenp": book_row["kenp"].iloc[0],
    }

    # All books average
    avg = {
        "units": others["units"].mean() if len(others) > 0 else 0,
        "revenue": others["revenue"].mean() if len(others) > 0 else 0,
        "kenp": others["kenp"].mean() if len(others) > 0 else 0,
        "books": len(others),
    }

    # Genre average
    genre_others = others[others["genre"] == book_genre] if book_genre else pd.DataFrame()
    genre_avg = {
        "units": genre_others["units"].mean() if len(genre_others) > 0 else 0,
        "revenue": genre_others["revenue"].mean() if len(genre_others) > 0 else 0,
        "kenp": genre_others["kenp"].mean() if len(genre_others) > 0 else 0,
        "books": len(genre_others),
        "label": book_genre or "Unknown",
    }

    # Author average
    author_others = others[others["author"] == book_author] if book_author else pd.DataFrame()
    author_avg = {
        "units": author_others["units"].mean() if len(author_others) > 0 else 0,
        "revenue": author_others["revenue"].mean() if len(author_others) > 0 else 0,
        "kenp": author_others["kenp"].mean() if len(author_others) > 0 else 0,
        "books": len(author_others),
        "label": book_author or "Unknown",
    }

    percentiles = {}
    for key in ["units", "revenue", "kenp"]:
        percentiles[key] = compute_percentile(book[key], others[key].values)

    # Composite score: mean of 3 percentiles
    score = round(np.mean(list(percentiles.values())))
    percentiles_market = compute_percentiles_by_market(df_territory, eid)

    return {
        "book": book,
        "avg": avg,
        "genre_avg": genre_avg,
        "author_avg": author_avg,
        "percentiles": percentiles,
        "percentiles_market": percentiles_market,
        "score": score,
    }


# ═══════════════════════════════════════════════════════════════
# FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════

def fc(val):
    return f"£{int(round(val)):,}"


def fn(val):
    return f"{int(round(val)):,}"

def status(book_val, avg_val):
    """Return (color, bg_color, label, icon) for a metric comparison."""
    if avg_val == 0:
        return AMBER, AMBER_BG, "No data", "\u25CF"
    r = book_val / avg_val
    if r >= 1.15:
        return GREEN, GREEN_BG, "Above avg", "\u25B2"
    elif r >= 0.85:
        return AMBER, AMBER_BG, "On track", "\u25CF"
    return RED, RED_BG, "Below avg", "\u25BC"


def pct_of_avg(book_val, avg_val):
    if avg_val == 0:
        return "--"
    return f"{round(book_val / avg_val * 100)}%"


def score_label(score):
    if score >= 80:
        return "Strong performer"
    elif score >= 60:
        return "Above average"
    elif score >= 40:
        return "Average"
    elif score >= 20:
        return "Below average"
    return "Needs attention"


def rounded_rect(ax, x, y, w, h, color, radius=0.012):
    r = mpatches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=color, edgecolor="none",
    )
    ax.add_patch(r)


# ═══════════════════════════════════════════════════════════════
# VISUALISATION
# ═══════════════════════════════════════════════════════════════

def generate_scorecard(title, edition_id, author, pub_date, genre, subgenre,
                       period_key, scorecard):
    """
    Generate a single scorecard image for one book at one milestone.
    """
    book = scorecard["book"]
    avg = scorecard["avg"]
    genre_avg = scorecard["genre_avg"]
    author_avg = scorecard["author_avg"]
    pctiles = scorecard["percentiles"]
    score = scorecard["score"]
    pctiles_market = scorecard.get("percentiles_market", {})

    pcfg = PERIOD_CONFIG[period_key]
    period_label = pcfg["label"]
    period_color = pcfg["color"]

    n_markets = sum(1 for m in ["GB", "US"] if m in pctiles_market)

    fig_w = 10
    base_h = 7.0
    market_section_h = n_markets * 1.2 + 0.5 if n_markets > 0 else 0
    fig_h = base_h + market_section_h
    
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    fig.patch.set_facecolor(C["bg"])
    ax.set_facecolor(C["bg"])

    # Scale factor: how much vertical space each "unit" takes in normalized coords
    # Base design was for fig_h=7.0, so we scale all y-gaps accordingly
    y_scale = base_h / fig_h

    # ── SECTION 0: Header bar ─────────────────────────────────
    header_h_frac = 0.10 * y_scale
    header_rect = mpatches.FancyBboxPatch(
        (0.02, 1 - header_h_frac - 0.01), 0.96, header_h_frac,
        boxstyle="round,pad=0.006",
        facecolor=C["header_bg"], edgecolor="none",
    )
    ax.add_patch(header_rect)

    display_title = title if len(title) < 40 else title[:37] + "..."
    ax.text(0.04, 1 - header_h_frac / 2 - 0.01,
            f"{display_title}  by {author}",
            fontsize=11, fontweight="bold", color="white",
            va="center", fontfamily="sans-serif")

    # Period badge on right
    badge_w = 0.12
    badge_h = 0.032 * y_scale
    badge = mpatches.FancyBboxPatch(
        (0.96 - badge_w - 0.01, 1 - header_h_frac / 2 - badge_h / 2 - 0.01),
        badge_w, badge_h,
        boxstyle="round,pad=0.005",
        facecolor=period_color, edgecolor="none",
    )
    ax.add_patch(badge)
    ax.text(0.96 - badge_w / 2 - 0.01, 1 - header_h_frac / 2 - 0.01,
            period_label.upper(), fontsize=7.5, fontweight="bold",
            color="white", ha="center", va="center", fontfamily="sans-serif")

    # Subtitle line
    pub_date_obj = pub_date if isinstance(pub_date, date) else pd.to_datetime(pub_date).date()
    days_since = (date.today() - pub_date_obj).days
    genre_display = subgenre if subgenre and str(subgenre) != "nan" else genre
    subtitle_y = 1 - header_h_frac - 0.015 * y_scale
    ax.text(0.04, subtitle_y,
            f"Published {pub_date_obj.strftime('%d %b %Y')}  ·  {days_since} days ago  ·  {genre_display}  ·  Edition {edition_id}",
            fontsize=8, color=C["grey_light"], fontfamily="sans-serif")

    # ═══════════════════════════════════════════════════════════
    # SECTION 1: Score ring + summary text
    # ═══════════════════════════════════════════════════════════
    s1_top = subtitle_y - 0.020

    # Score ring — drawn in its own inset axes for perfect circle
    ring_size = 0.10
    ring_y_center = s1_top - 0.06
    ring_inset = fig.add_axes([0.025, ring_y_center - ring_size / 2,
                                ring_size, ring_size * fig_w / fig_h])
    ring_inset.set_xlim(-1.3, 1.3)
    ring_inset.set_ylim(-1.3, 1.3)
    ring_inset.set_aspect("equal")
    ring_inset.axis("off")
    ring_inset.patch.set_alpha(0)

    # Background track
    bg_circle = plt.Circle((0, 0), 1, fill=False,
                            edgecolor=C["bar_track"], linewidth=5)
    ring_inset.add_patch(bg_circle)

    # Score arc
    ring_color = GREEN if score >= 60 else AMBER if score >= 35 else RED
    theta = np.linspace(np.pi / 2, np.pi / 2 - (score / 100) * 2 * np.pi, 100)
    ring_inset.plot(np.cos(theta), np.sin(theta),
                    color=ring_color, linewidth=5, solid_capstyle="round")

    ring_inset.text(0, 0.1, str(score), fontsize=22, fontweight="bold",
                    ha="center", va="center", color=C["text_dark"])
    ring_inset.text(0, -0.35, "out of 100", fontsize=7,
                    ha="center", va="center", color=C["text_light"])

    # Summary text next to ring
    s_label = score_label(score)
    ax.text(0.16, s1_top - 0.01, s_label,
            fontsize=15, fontweight="bold", color=C["text_dark"],
            va="center", fontfamily="sans-serif")

    # Build summary sentence
    metric_names = {"revenue": "Revenue", "units": "Units sold", "kenp": "KENP reads"}
    best_key = max(["revenue", "units", "kenp"],
                   key=lambda k: book[k] / avg[k] if avg[k] > 0 else 0)
    worst_key = min(["revenue", "units", "kenp"],
                    key=lambda k: book[k] / avg[k] if avg[k] > 0 else 999)

    summary_parts = [
        f"At {period_label}, total revenue is {fc(book['revenue'])} "
        f"vs the catalogue average of {fc(avg['revenue'])}.",
    ]
    if avg[best_key] > 0:
        summary_parts.append(
            f"Standout: {metric_names[best_key]} at "
            f"{pct_of_avg(book[best_key], avg[best_key])} of average."
        )
    if avg[worst_key] > 0 and best_key != worst_key:
        summary_parts.append(
            f"Watch: {metric_names[worst_key]} at "
            f"{pct_of_avg(book[worst_key], avg[worst_key])} of average."
        )

    import textwrap
    summary = " ".join(summary_parts)
    wrapped = textwrap.fill(summary, width=72)
    ax.text(0.16, s1_top - 0.035, wrapped,
            fontsize=8.5, color=C["text_light"], va="top",
            linespacing=1.5, fontfamily="sans-serif")

    # ═══════════════════════════════════════════════════════════
    # SECTION 2: Metric health cards — 3 rows (All / Genre / Author) × 3 metrics
    # ═══════════════════════════════════════════════════════════
    s2_top = s1_top - 0.13
    ax.text(0.03, s2_top, f"Metric health at {period_label}",
            fontsize=10.5, fontweight="bold", color=C["text_light"],
            fontfamily="sans-serif")

    card_w = 0.305
    card_h = 0.055 * y_scale
    gap_x = 0.015
    gap_y = 0.010 * y_scale
    row_gap = 0.025 * y_scale
    cards_x = 0.03

    metrics_config = [
        ("Revenue", "revenue", fc),
        ("Units sold", "units", fn),
        ("KENP reads", "kenp", fn),
    ]

    benchmarks = [
        ("vs All Books", avg, f"{avg['books']} books", "#0984E3"),
        ("vs Genre", genre_avg, f"{genre_avg['books']} in {genre_avg['label']}", "#00B894"),
        ("vs Author", author_avg, f"{author_avg['books']} by {author_avg['label']}", "#8B5CF6"),
    ]

    cursor_y = s2_top - 0.03 * y_scale

    for row_idx, (bench_label, bench_data, bench_sub, bench_color) in enumerate(benchmarks):
        # Row label
        ax.text(0.03, cursor_y, bench_label,
                fontsize=8.5, fontweight="bold", color=bench_color,
                fontfamily="sans-serif")
        ax.text(0.17, cursor_y, f"({bench_sub})",
                fontsize=7.5, color=C["grey_light"],
                fontfamily="sans-serif")

        cursor_y -= 0.012 * y_scale

        for i, (label, key, fmt) in enumerate(metrics_config):
            x = cards_x + i * (card_w + gap_x)
            y = cursor_y - card_h

            if bench_data["books"] == 0:
                # No comparison data — grey card
                rounded_rect(ax, x, y, card_w, card_h, C["bar_track"])
                ax.text(x + 0.015, y + card_h * 0.65, label,
                        fontsize=7.5, color=C["text_light"], va="center",
                        fontfamily="sans-serif")
                ax.text(x + 0.015, y + card_h * 0.28, "—",
                        fontsize=14, fontweight="bold", color=C["text_light"],
                        va="center", fontfamily="sans-serif")
                ax.text(x + card_w - 0.015, y + card_h / 2,
                        "No data", fontsize=8, color=C["text_light"],
                        ha="right", va="center", fontfamily="sans-serif")
            else:
                col, bg, txt, icon = status(book[key], bench_data[key])
                rounded_rect(ax, x, y, card_w, card_h, bg)

                ax.text(x + 0.015, y + card_h * 0.72, label,
                        fontsize=7.5, color=C["text_light"], va="center",
                        fontfamily="sans-serif")

                pct_text = pct_of_avg(book[key], bench_data[key])
                ax.text(x + 0.015, y + card_h * 0.28, pct_text,
                        fontsize=14, fontweight="bold", color=C["text_dark"],
                        va="center", fontfamily="sans-serif")

                ax.text(x + card_w - 0.015, y + card_h * 0.55,
                        f"{icon} {txt}", fontsize=8, fontweight="bold",
                        color=col, ha="right", va="center", fontfamily="sans-serif")

                ax.text(x + card_w - 0.015, y + card_h * 0.22,
                        f"{fmt(book[key])} vs {fmt(bench_data[key])}",
                        fontsize=6.5, color=C["grey_light"],
                        ha="right", va="center", fontfamily="sans-serif")

        cursor_y -= card_h + row_gap

    # ═══════════════════════════════════════════════════════════
    # SECTION 3: Percentile bars (vs all books catalogue)
    # ═══════════════════════════════════════════════════════════
    s3_top = cursor_y - 0.016 * y_scale
    ax.text(0.03, s3_top, "Percentile rank vs catalogue",
            fontsize=10.5, fontweight="bold", color=C["text_light"],
            fontfamily="sans-serif")

    bar_left, bar_right = 0.18, 0.82
    bar_h_norm = 0.024 * y_scale
    bar_gap = 0.045 * y_scale

    bar_metrics = [
        ("Revenue", "revenue"),
        ("Units sold", "units"),
        ("KENP reads", "kenp"),
    ]

    cursor_y = s3_top - 0.035 * y_scale

    for i, (label, key) in enumerate(bar_metrics):
        y = cursor_y - i * bar_gap
        p = pctiles[key]

        col = GREEN if p >= 65 else (AMBER if p >= 35 else RED)

        ax.text(bar_left - 0.015, y + bar_h_norm / 2, label,
                fontsize=9, color=C["text_light"], ha="right",
                va="center", fontfamily="sans-serif")

        # Background
        rounded_rect(ax, bar_left, y, bar_right - bar_left,
                     bar_h_norm, C["bar_track"], radius=0.008)

        # Filled bar
        total_w = bar_right - bar_left
        filled = total_w * (p / 100)

        if p > 0:
            filled = max(filled, 0.015)
        else:
            filled = 0

        radius = 0.008 if filled > 0.04 else 0.002
        rounded_rect(ax, bar_left, y, filled,
                     bar_h_norm, col, radius=radius)

        rank_label = f"Top {100 - p}%" if p > 50 else f"Bottom {p}%"
        ax.text(bar_right + 0.015, y + bar_h_norm / 2, rank_label,
                fontsize=9.5, fontweight="bold", color=C["text_dark"],
                va="center", fontfamily="sans-serif")

    cursor_y = cursor_y - len(bar_metrics) * bar_gap

    # ═══════════════════════════════════════════════════════════
    # SECTION 4: Percentile rank by market (GB / US)
    # ═══════════════════════════════════════════════════════════
    if n_markets > 0:
        s4_top = cursor_y - 0.029 * y_scale
        ax.text(0.03, s4_top, "Percentile rank by market",
                fontsize=10.5, fontweight="bold",
                color=C["text_light"], fontfamily="sans-serif")

        market_bar_gap = 0.040 * y_scale
        market_section_gap = 0.025 * y_scale

        cursor_y = s4_top - 0.040 * y_scale

        markets = ["GB", "US"]

        

        for market in markets:
            if market not in pctiles_market:
                continue

            market_pct = pctiles_market[market]

            # Market label
            ax.text(0.05, cursor_y , market,
                    fontsize=9.5, fontweight="bold",
                    color=C["text_dark"], fontfamily="sans-serif")

            cursor_y -= 0.02  # spacing after label

            for label, key in bar_metrics:
                p = market_pct[key]

                col = GREEN if p >= 65 else (AMBER if p >= 35 else RED)

                # Label
                ax.text(bar_left - 0.015, cursor_y + bar_h_norm / 2, label,
                        fontsize=8.5, color=C["text_light"], ha="right",
                        va="center", fontfamily="sans-serif")

                # Background
                rounded_rect(ax, bar_left, cursor_y,
                            bar_right - bar_left, bar_h_norm,
                            C["bar_track"], radius=0.008)

                # Filled
                total_w = bar_right - bar_left
                filled = total_w * (p / 100)

                if p > 0:
                    filled = max(filled, 0.015)
                else:
                    filled = 0

                radius = 0.008 if filled > 0.04 else 0.002

                rounded_rect(ax, bar_left, cursor_y,
                            filled, bar_h_norm, col, radius=radius)

                rank_label = f"Top {100 - p}%" if p > 50 else f"Bottom {p}%"

                ax.text(bar_right + 0.015, cursor_y + bar_h_norm / 2,
                        rank_label, fontsize=9,
                        fontweight="bold", color=C["text_dark"],
                        va="center", fontfamily="sans-serif")

                cursor_y -= 0.045  # spacing between rows

            cursor_y -= 0.03  # spacing between markets

    # ═══════════════════════════════════════════════════════════
    # Footer
    # ═══════════════════════════════════════════════════════════
    footer_y = cursor_y - 0.02 * y_scale
    footer_y = max(0.02, footer_y)  # Ensure it stays on canvas
    
    ax.text(0.03, footer_y,
            f"{period_label} performance summary  ·  vs {avg['books']} books in catalogue",
            fontsize=8, color=C["grey_light"], style="italic",
            fontfamily="sans-serif")

    # ── Save ──
    plt.subplots_adjust(left=0.02, right=0.98, top=0.98,
                        bottom=max(0.01, footer_y - 0.02))

    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in title).strip()
    safe_title = safe_title.replace(" ", "_")[:40]
    filename = f"{OUTPUT_DIR}/{safe_title}_{edition_id}_{period_key}.png"
    fig.savefig(filename, dpi=200, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)

    print(f"  ✓ {filename}")
    return filename

# ═══════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════

def generate_all(edition_id=None, milestone=None):
    """
    Generate launch scorecards.

    Args:
        edition_id: Generate for a specific book (optional).
        milestone: '30d', '90d', or '12m' — generate just one milestone.
                   If None, generates all milestones the book qualifies for.
    """
    print("Fetching launch data...")

    datasets = {}

    milestones_to_run = [milestone] if milestone else ["30d", "90d", "12m"]

    if "30d" in milestones_to_run:
        print("  Fetching 30-day daily sales...")
        d30 = fetch_daily_launch_data(30)
        k30 = fetch_daily_kenp_data(30)
        d30 = merge_kenp(d30, k30)
        datasets["30d"] = build_totals(d30)

    if "90d" in milestones_to_run:
        print("  Fetching 90-day daily sales...")
        d90 = fetch_daily_launch_data(90)
        k90 = fetch_daily_kenp_data(90)
        d90 = merge_kenp(d90, k90)
        datasets["90d"] = build_totals(d90)

    if "12m" in milestones_to_run:
        print("  Fetching 12-month monthly sales...")
        m12 = fetch_monthly_launch_data()
        if not m12.empty:
            datasets["12m"] = build_totals(m12)

    if not datasets:
        print("No data found.")
        return []

    today = date.today()

    # Ensure pub_date is parsed in all datasets
    for mk, data in datasets.items():
        df_global = data["global"]

        if df_global is not None and not df_global.empty and "pub_date" in df_global.columns:
            datasets[mk]["global"]["pub_date"] = pd.to_datetime(df_global["pub_date"]).dt.date

    if edition_id:
        # Find book metadata from whichever dataset has it
        target_row = None
        for data in datasets.values():
            df = data["global"]
            if df is None or df.empty:
                continue
            match = df[df["edition_id"] == edition_id]
            if not match.empty:
                target_row = match.iloc[0]
                break

        if target_row is None:
            print(f"Edition {edition_id} not found in any dataset.")
            return []

        print(f"Generating report for edition {edition_id}\n")
    else:
        # Default: for each milestone, find books that JUST hit it
        # (published exactly 30, 90, or 365 days ago ±2 days)
        # Each book only gets the card for the milestone it just crossed.
        milestone_targets = {}  # {period_key: DataFrame of target books}

        for mk in milestones_to_run:
            df = datasets.get(mk)
            if df is None:
                continue
            df = df["global"]
            if df is None or df.empty:
                continue

            df = df.copy()
            df["pub_date"] = pd.to_datetime(df["pub_date"]).dt.date

            target_days = PERIOD_CONFIG[mk]["days"]
            exact_date = today - timedelta(days=target_days)
            hits = df[df["pub_date"] == exact_date][
                ["edition_id", "title", "author", "pub_date", "genre",
                 "genre_subgenre", "pub_month"]
            ].drop_duplicates(subset=["edition_id"])

            if not hits.empty:
                milestone_targets[mk] = hits

        if not milestone_targets:
            print("No books found hitting a milestone today.")
            return []

        total_books = sum(len(df) for df in milestone_targets.values())
        print(f"Found {total_books} book-milestone(s)\n")

    filenames = []

    if edition_id:
        # Specific book: generate all qualifying milestones
        days_since = (today - target_row["pub_date"]).days

        for period_key in milestones_to_run:
            if days_since < PERIOD_CONFIG[period_key]["days"]:
                continue

            df = datasets.get(period_key)
            if df is None:
                continue

            df_global = df["global"]

            if df_global is None or df_global.empty:
                continue

            if df_global[df_global["edition_id"] == edition_id].empty:
                continue

            df_territory = datasets[period_key]["by_territory"]

            

            scorecard = build_scorecard(df_global, df_territory, edition_id)
            if scorecard is None:
                continue

            print(f"📖 {target_row['title']} by {target_row['author']} — {period_key} "
                  f"(score: {scorecard['score']})")

            fname = generate_scorecard(
                target_row["title"], edition_id, target_row["author"],
                target_row["pub_date"], target_row["genre"],
                target_row["genre_subgenre"], period_key, scorecard
            )
            if fname:
                filenames.append(fname)
    else:
        # Auto mode: each book only gets the milestone it just crossed
        for period_key, hits in milestone_targets.items():
            df = datasets[period_key]["global"]

            for _, row in hits.iterrows():
                eid = row["edition_id"]
                df_territory = datasets[period_key]["by_territory"]

                scorecard = build_scorecard(df, df_territory, eid)
                if scorecard is None:
                    continue

                print(f"📖 {row['title']} by {row['author']} — {period_key} "
                      f"(score: {scorecard['score']})")

                fname = generate_scorecard(
                    row["title"], eid, row["author"], row["pub_date"],
                    row["genre"], row["genre_subgenre"], period_key, scorecard
                )
                if fname:
                    filenames.append(fname)

    print(f"\nDone! {len(filenames)} scorecards in '{OUTPUT_DIR}/'")
    return filenames


if __name__ == "__main__":
    import sys

    edition_id = None
    milestone = None

    for arg in sys.argv[1:]:
        if arg in ("30d", "90d", "12m"):
            milestone = arg
        else:
            try:
                edition_id = int(arg)
            except ValueError:
                pass

    generate_all(edition_id=edition_id, milestone=milestone)