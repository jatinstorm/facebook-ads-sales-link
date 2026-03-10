# generate_scorecards.py
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

# ── Configuration ──────────────────────────────────────────────
OUTPUT_DIR = "scorecards"
os.makedirs(OUTPUT_DIR, exist_ok=True)

COLORS = {
    "bg": "#faf9f6",
    "card_bg": "#ffffff",
    "header_bg": "#2d3436",
    "accent": "#e94560",
    "accent_green": "#00b894",
    "accent_amber": "#e17055",
    "text_white": "#ffffff",
    "text_dark": "#2d3436",
    "text_light": "#636e72",
    "text_muted": "#b2bec3",
    "positive": "#00b894",
    "negative": "#d63031",
    "row_alt": "#f1f0eb",
    "daily_label": "#e94560",
    "cumulative_label": "#0984e3",
}
METRIC_NAMES = [
    "Clicks", "Ad Spend", "CPC", "CTR", "Impressions",
    "Ebook Units", "Paperback Units", "KENP",
    "Total Revenue", "Publisher Revenue", "Gross Profit", "Ad Spend % of Rev",
]


def fetch_scorecard_data(date=None):
    """Fetch daily + cumulative (current run only) data."""
    client = get_client()

    date_filter = (
        f"DATE('{date}')" if date else "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
    )

    daily_query = f"""
    SELECT
        Title, Edition_ID, Territory, adset_name,
        SUM(spend) AS spend, SUM(clicks) AS clicks,
        AVG(cpc) AS cpc, AVG(ctr) AS ctr,
        SUM(impressions) AS impressions,
        SUM(ebook_units) AS ebook_units,
        SUM(paperback_units) AS paperback_units,
        SUM(kenp) AS kenp,
        SUM(ebook_revenue) AS ebook_revenue,
        SUM(paperback_revenue) AS paperback_revenue
    FROM `marketing-489109.facebook_ads.ads_sales_analytics`
    WHERE date_start = {date_filter}
    GROUP BY Title, Edition_ID, Territory, adset_name
    ORDER BY Title, Territory, adset_name
    """

    cumulative_query = f"""
    WITH run_dates AS (
        SELECT
            Title, Edition_ID, Territory, adset_name, date_start,
            DATE_DIFF(date_start, LAG(date_start) OVER (
                PARTITION BY Edition_ID, Territory, adset_name
                ORDER BY date_start
            ), DAY) AS days_since_prev
        FROM `marketing-489109.facebook_ads.ads_sales_analytics`
        WHERE date_start <= {date_filter}
    ),
    run_starts AS (
        SELECT
            Edition_ID, Territory, adset_name,
            MAX(date_start) AS current_run_start
        FROM run_dates
        WHERE days_since_prev IS NULL OR days_since_prev > 1
        GROUP BY Edition_ID, Territory, adset_name
    )
    SELECT
        a.Title, a.Edition_ID, a.Territory, a.adset_name,
        SUM(a.spend) AS spend, SUM(a.clicks) AS clicks,
        AVG(a.cpc) AS cpc, AVG(a.ctr) AS ctr,
        SUM(a.impressions) AS impressions,
        SUM(a.ebook_units) AS ebook_units,
        SUM(a.paperback_units) AS paperback_units,
        SUM(a.kenp) AS kenp,
        SUM(a.ebook_revenue) AS ebook_revenue,
        SUM(a.paperback_revenue) AS paperback_revenue
    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a
    JOIN run_starts r
        ON a.Edition_ID = r.Edition_ID
        AND a.Territory = r.Territory
        AND a.adset_name = r.adset_name
    WHERE a.date_start >= r.current_run_start
        AND a.date_start <= {date_filter}
    GROUP BY a.Title, a.Edition_ID, a.Territory, a.adset_name
    ORDER BY a.Title, a.Territory, a.adset_name
    """

    daily_df = client.query(daily_query).to_dataframe()
    cumulative_df = client.query(cumulative_query).to_dataframe()

    # Only show campaigns that were active yesterday
    if not daily_df.empty and not cumulative_df.empty:
        active_keys = daily_df[["Edition_ID", "Territory", "adset_name"]].drop_duplicates()
        cumulative_df = cumulative_df.merge(active_keys, on=["Edition_ID", "Territory", "adset_name"], how="inner")

    return daily_df, cumulative_df


def format_currency(val, symbol="£"):
    if abs(val) >= 1000:
        return f"{symbol}{val:,.0f}"
    elif abs(val) >= 1:
        return f"{symbol}{val:,.2f}"
    else:
        return f"{symbol}{val:.2f}"


def format_number(val):
    if val == int(val):
        return f"{int(val):,}"
    return f"{val:,.2f}"


def format_pct(val):
    return f"{val:.1f}%"


def compute_summary(data):
    """Compute territory summary metrics from a filtered DataFrame."""
    if data.empty:
        return {m: ("—", 0) for m in METRIC_NAMES}

    total_spend = data["spend"].sum()
    total_clicks = data["clicks"].sum()
    avg_cpc = total_spend / total_clicks if total_clicks > 0 else 0
    total_impressions = data["impressions"].sum()
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    total_ebook_rev = data["ebook_revenue"].sum()
    total_pb_rev = data["paperback_revenue"].sum()
    total_revenue = total_ebook_rev + total_pb_rev
    publisher_revenue = total_revenue * 0.5
    gross_profit = publisher_revenue - total_spend
    ad_spend_pct = (total_spend / total_revenue * 100) if total_revenue > 0 else 0

    return {
        "Clicks": (format_number(total_clicks), total_clicks),
        "Ad Spend": (format_currency(total_spend), total_spend),
        "CPC": (format_currency(avg_cpc), avg_cpc),
        "CTR": (format_pct(avg_ctr), avg_ctr),
        "Impressions": (format_number(total_impressions), total_impressions),
        "Ebook Units": (format_number(data["ebook_units"].sum()), data["ebook_units"].sum()),
        "Paperback Units": (format_number(data["paperback_units"].sum()), data["paperback_units"].sum()),
        "KENP": (format_number(data["kenp"].sum()), data["kenp"].sum()),
        "Total Revenue": (format_currency(total_revenue), total_revenue),
        "Publisher Revenue": (format_currency(publisher_revenue), publisher_revenue),
        "Gross Profit": (format_currency(gross_profit), gross_profit),
        "Ad Spend % of Rev": (format_pct(ad_spend_pct), ad_spend_pct),
    }


def get_metric_color(metric, raw_val):
    if metric == "Gross Profit":
        return COLORS["positive"] if raw_val >= 0 else COLORS["negative"]
    elif metric == "Ad Spend % of Rev":
        if raw_val < 50:
            return COLORS["positive"]
        elif raw_val < 80:
            return COLORS["accent_amber"]
        return COLORS["negative"]
    return COLORS["text_dark"]


def generate_book_scorecard(title, edition_id, daily_data, cumulative_data):
    """Generate a scorecard image for one book with daily + cumulative columns."""

    territories = sorted(
        set(daily_data["Territory"].unique().tolist() if not daily_data.empty else []) |
        set(cumulative_data["Territory"].unique().tolist() if not cumulative_data.empty else [])
    )

    if not territories:
        return None

    # ── Compute summaries ──
    daily_summaries = {}
    cumulative_summaries = {}
    for terr in territories:
        d = daily_data[daily_data["Territory"] == terr] if not daily_data.empty else pd.DataFrame()
        c = cumulative_data[cumulative_data["Territory"] == terr] if not cumulative_data.empty else pd.DataFrame()
        daily_summaries[terr] = compute_summary(d)
        cumulative_summaries[terr] = compute_summary(c)

    # ── Per-adset breakdown (daily) ──
    adset_data = {}
    for terr in territories:
        if daily_data.empty:
            adset_data[terr] = []
            continue
        t_data = daily_data[daily_data["Territory"] == terr]
        adsets = []
        for _, row in t_data.iterrows():
            adset_rev = row["ebook_revenue"] + row["paperback_revenue"]
            adsets.append({
                "name": row["adset_name"],
                "spend": format_currency(row["spend"]),
                "clicks": format_number(row["clicks"]),
                "revenue": format_currency(adset_rev),
            })
        adset_data[terr] = adsets

    n_territories = len(territories)
    n_metrics = len(METRIC_NAMES)
    max_adsets = max((len(adset_data.get(t, [])) for t in territories), default=0)

    # ── Figure dimensions ──
    row_height = 0.42
    header_height = 1.2
    sub_col_width = 2.4
    territory_width = sub_col_width * 2 + 0.3
    label_width = 3.0
    padding = 0.6

    total_rows = n_metrics + (2 + max_adsets if max_adsets > 0 else 0)
    fig_height = header_height + (total_rows * row_height) + 1.8
    fig_width = label_width + (n_territories * territory_width) + padding

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    fig.patch.set_facecolor(COLORS["bg"])
    ax.set_facecolor(COLORS["bg"])
    ax.set_xlim(0, fig_width)
    ax.set_ylim(0, fig_height)
    ax.axis("off")

    # ── Title bar ──
    title_rect = mpatches.FancyBboxPatch(
        (0.2, fig_height - header_height),
        fig_width - 0.4, header_height - 0.15,
        boxstyle="round,pad=0.1",
        facecolor=COLORS["header_bg"],
        edgecolor=COLORS["accent"],
        linewidth=2,
    )
    ax.add_patch(title_rect)

    display_title = title if len(title) < 50 else title[:47] + "..."
    ax.text(
        0.5, fig_height - 0.45,
        display_title,
        fontsize=16, fontweight="bold", color=COLORS["text_white"],
        fontfamily="monospace",
    )
    ax.text(
        0.5, fig_height - 0.85,
        f"Edition ID: {edition_id}  •  Daily + Cumulative Scorecard",
        fontsize=9, color=COLORS["text_light"],
        fontfamily="monospace",
    )

    # ── Territory + sub-column headers ──
    y_start = fig_height - header_height - 0.15

    for i, terr in enumerate(territories):
        terr_x_start = label_width + (i * territory_width)
        terr_x_center = terr_x_start + (territory_width / 2)

        # Territory name
        hdr_rect = mpatches.FancyBboxPatch(
            (terr_x_start + 0.1, y_start - 0.35),
            territory_width - 0.2, 0.4,
            boxstyle="round,pad=0.05",
            facecolor=COLORS["accent"],
            edgecolor="none",
        )
        ax.add_patch(hdr_rect)
        ax.text(
            terr_x_center, y_start - 0.15,
            terr,
            fontsize=11, fontweight="bold", color=COLORS["text_white"],
            ha="center", va="center", fontfamily="monospace",
        )

        # Sub-column pills
        daily_x = terr_x_start + (sub_col_width / 2) + 0.05
        cumul_x = terr_x_start + sub_col_width + 0.25 + (sub_col_width / 2)
        sub_label_y = y_start - 0.65
        pill_w = sub_col_width - 0.2

        # Yesterday pill
        pill = mpatches.FancyBboxPatch(
            (daily_x - pill_w / 2, sub_label_y - 0.13),
            pill_w, 0.28,
            boxstyle="round,pad=0.04",
            facecolor=COLORS["daily_label"] + "33",
            edgecolor=COLORS["daily_label"],
            linewidth=1,
        )
        ax.add_patch(pill)
        ax.text(daily_x, sub_label_y, "Yesterday",
                fontsize=8, fontweight="bold", color=COLORS["daily_label"],
                ha="center", va="center", fontfamily="monospace")

        # All Time pill
        pill2 = mpatches.FancyBboxPatch(
            (cumul_x - pill_w / 2, sub_label_y - 0.13),
            pill_w, 0.28,
            boxstyle="round,pad=0.04",
            facecolor=COLORS["cumulative_label"] + "33",
            edgecolor=COLORS["cumulative_label"],
            linewidth=1,
        )
        ax.add_patch(pill2)
        ax.text(cumul_x, sub_label_y, "All Time",
                fontsize=8, fontweight="bold", color=COLORS["cumulative_label"],
                ha="center", va="center", fontfamily="monospace")

    # ── Metric rows ──
    y_cursor = y_start - 1.05

    for idx, metric in enumerate(METRIC_NAMES):
        row_color = COLORS["row_alt"] if idx % 2 == 0 else COLORS["card_bg"]
        row_rect = mpatches.Rectangle(
            (0.2, y_cursor - 0.18),
            fig_width - 0.4, row_height,
            facecolor=row_color, edgecolor="none",
        )
        ax.add_patch(row_rect)

        ax.text(0.45, y_cursor + 0.03, metric,
                fontsize=10, color=COLORS["text_light"],
                va="center", fontfamily="monospace")

        for i, terr in enumerate(territories):
            terr_x_start = label_width + (i * territory_width)
            daily_x = terr_x_start + (sub_col_width / 2) + 0.05
            cumul_x = terr_x_start + sub_col_width + 0.25 + (sub_col_width / 2)

            # Daily
            d_display, d_raw = daily_summaries[terr][metric]
            ax.text(daily_x, y_cursor + 0.03, d_display,
                    fontsize=10, fontweight="bold",
                    color=get_metric_color(metric, d_raw),
                    ha="center", va="center", fontfamily="monospace")

            # Cumulative
            c_display, c_raw = cumulative_summaries[terr][metric]
            ax.text(cumul_x, y_cursor + 0.03, c_display,
                    fontsize=10, fontweight="bold",
                    color=get_metric_color(metric, c_raw),
                    ha="center", va="center", fontfamily="monospace")

        y_cursor -= row_height

    # ── Adset breakdown (daily) ──
    if max_adsets > 0:
        y_cursor -= 0.2
        ax.text(0.45, y_cursor + 0.03, "ADSET BREAKDOWN (Yesterday)",
                fontsize=9, fontweight="bold", color=COLORS["accent"],
                va="center", fontfamily="monospace")
        y_cursor -= row_height

        for a_idx in range(max_adsets):
            row_color = COLORS["row_alt"] if a_idx % 2 == 0 else COLORS["card_bg"]
            row_rect = mpatches.Rectangle(
                (0.2, y_cursor - 0.18),
                fig_width - 0.4, row_height,
                facecolor=row_color, edgecolor="none",
            )
            ax.add_patch(row_rect)

            # Adset name from first territory
            first_adsets = adset_data.get(territories[0], [])
            if a_idx < len(first_adsets):
                name = first_adsets[a_idx]["name"]
                if len(name) > 25:
                    name = name[:22] + "..."
                ax.text(0.45, y_cursor + 0.03, name,
                        fontsize=8, color=COLORS["text_muted"],
                        va="center", fontfamily="monospace")

            for i, terr in enumerate(territories):
                adsets = adset_data.get(terr, [])
                if a_idx < len(adsets):
                    adset = adsets[a_idx]
                    terr_x_start = label_width + (i * territory_width)
                    center_x = terr_x_start + (territory_width / 2)
                    summary = f"{adset['spend']}  |  {adset['clicks']} cl  |  {adset['revenue']}"
                    ax.text(center_x, y_cursor + 0.03, summary,
                            fontsize=8, color=COLORS["text_light"],
                            ha="center", va="center", fontfamily="monospace")

            y_cursor -= row_height

    # ── Bottom accent line ──
    ax.plot([0.3, fig_width - 0.3], [0.3, 0.3],
            color=COLORS["accent"], linewidth=2)

    plt.tight_layout(pad=0.3)

    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in title).strip()
    safe_title = safe_title.replace(" ", "_")[:50]
    filename = f"{OUTPUT_DIR}/{safe_title}_{edition_id}.png"
    fig.savefig(filename, dpi=150, bbox_inches="tight", facecolor=COLORS["bg"])
    plt.close(fig)

    print(f"  ✓ Saved: {filename}")
    return filename


def generate_all_scorecards(date=None):
    """Generate scorecards for all books."""
    print("Fetching data...")
    daily_df, cumulative_df = fetch_scorecard_data(date)

    if daily_df.empty and cumulative_df.empty:
        print("No data found for the specified date.")
        return []

    all_books = set()
    for df in [daily_df, cumulative_df]:
        if not df.empty:
            for _, row in df[["Title", "Edition_ID"]].drop_duplicates().iterrows():
                all_books.add((row["Title"], row["Edition_ID"]))

    print(f"Found {len(all_books)} books\n")

    filenames = []
    for title, edition_id in sorted(all_books):
        print(f"Generating scorecard: {title} (#{edition_id})")
        d_data = daily_df[daily_df["Edition_ID"] == edition_id] if not daily_df.empty else pd.DataFrame()
        c_data = cumulative_df[cumulative_df["Edition_ID"] == edition_id] if not cumulative_df.empty else pd.DataFrame()
        fname = generate_book_scorecard(title, edition_id, d_data, c_data)
        if fname:
            filenames.append(fname)

    print(f"\nDone! Generated {len(filenames)} scorecards in '{OUTPUT_DIR}/'")
    return filenames


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else None
    generate_all_scorecards(date)