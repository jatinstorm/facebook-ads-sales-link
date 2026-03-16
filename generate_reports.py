# generate_weekly_report.py
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import numpy as np
import pandas as pd
import os
from datetime import timedelta

try:
    from bq import get_client
except ImportError:
    get_client = None


OUTPUT_DIR = os.getenv("WEEKLY_OUTPUT_DIR", "weekly_reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)


C = {
    "bg": "#FEFCF8",
    "card": "#FFFFFF",
    "header_bg": "#1B1B2F",
    "accent": "#E94560",
    "accent2": "#0984E3",
    "gold": "#F4C430",
    "text_dark": "#1B1B2F",
    "text_mid": "#555555",
    "text_light": "#999999",
    "chart_blue": "#0984E3",
    "chart_red": "#E94560",
    "chart_gold": "#F4C430",
    "chart_green": "#00B894",
    "grid": "#EEEEEE",
    "table_row2": "#F7F5F0",
    "divider": "#E0DCD5",
}


# ------------------------------------------------------------
# DATA
# ------------------------------------------------------------

def fetch_weekly_data(date=None):

    if get_client is None:
        raise RuntimeError("bq.get_client() not available")

    client = get_client()

    end_date = f"DATE('{date}')" if date else "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"

    query = f"""
    WITH date_range AS (
        SELECT DATE_SUB({end_date}, INTERVAL 6 DAY) AS start_date,
               {end_date} AS end_date
    )
    SELECT
        a.Title,
        a.Edition_ID,
        a.Territory,
        a.date_start,

        SUM(a.spend) AS spend,
        SUM(a.clicks) AS clicks,
        SUM(a.impressions) AS impressions,

        SUM(a.ebook_units) AS ebook_units,
        SUM(a.paperback_units) AS paperback_units,
        SUM(a.kenp) AS kenp,

        SUM(a.ebook_revenue) AS ebook_revenue,
        SUM(a.paperback_revenue) AS paperback_revenue,
        SUM(a.kenp_revenue) AS kenp_revenue

    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a, date_range d
    WHERE a.date_start BETWEEN d.start_date AND d.end_date

    GROUP BY a.Title, a.Edition_ID, a.Territory, a.date_start
    ORDER BY a.Title, a.Territory, a.date_start
    """

    return client.query(query).to_dataframe()


# ------------------------------------------------------------
# FORMATTERS
# ------------------------------------------------------------

def format_currency(v):
    if abs(v) >= 1000:
        return f"£{v:,.0f}"
    if abs(v) >= 1:
        return f"£{v:,.2f}"
    return f"£{v:.2f}"


def format_number(v):
    if v == int(v):
        return f"{int(v):,}"
    return f"{v:,.1f}"


def format_pct(v):
    return f"{v:.1f}%"


# ------------------------------------------------------------
# SCORECARD
# ------------------------------------------------------------

def draw_scorecard(ax, label, value, subtitle=None, color=None):

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    card = mpatches.FancyBboxPatch(
        (0.02, 0.02), 0.96, 0.96,
        boxstyle="round,pad=0.04",
        facecolor=C["card"],
        edgecolor=C["divider"],
        linewidth=1
    )

    ax.add_patch(card)

    ax.plot([0.1, 0.9], [0.92, 0.92],
            color=color or C["accent"],
            linewidth=3)

    ax.text(0.5, 0.75, label,
            ha="center",
            fontsize=8,
            color=C["text_light"])

    ax.text(0.5, 0.48, value,
            ha="center",
            fontsize=18,
            color=color or C["text_dark"],
            fontweight="bold")

    if subtitle:
        ax.text(0.5, 0.2, subtitle,
                ha="center",
                fontsize=7,
                color=C["text_light"])


# ------------------------------------------------------------
# REPORT
# ------------------------------------------------------------

def generate_territory_report(title, edition_id, territory, df, dates):

    daily = {}

    for d in dates:

        day = df[df["date_start"] == d]

        if day.empty:
            continue

        spend = day["spend"].sum()
        clicks = day["clicks"].sum()
        impressions = day["impressions"].sum()

        ebook_rev = day["ebook_revenue"].sum()
        pb_rev = day["paperback_revenue"].sum()
        kenp_rev = day["kenp_revenue"].sum()

        daily[d] = {

            "spend": spend,
            "clicks": clicks,
            "impressions": impressions,

            "cpc": spend / clicks if clicks else 0,
            "ctr": (clicks / impressions * 100) if impressions else 0,

            "ebook_units": day["ebook_units"].sum(),
            "paperback_units": day["paperback_units"].sum(),
            "kenp": day["kenp"].sum(),

            "revenue": ebook_rev + pb_rev + kenp_rev
        }

    dates = list(daily.keys())

    if not dates:
        return None


# ------------------------------------------------------------
# WEEKLY TOTALS
# ------------------------------------------------------------

    total_clicks = sum(d["clicks"] for d in daily.values())
    total_spend = sum(d["spend"] for d in daily.values())
    total_impressions = sum(d["impressions"] for d in daily.values())
    total_revenue = sum(d["revenue"] for d in daily.values())

    avg_cpc = total_spend / total_clicks if total_clicks else 0
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions else 0

    publisher_rev = total_revenue * 0.5
    gross_profit = publisher_rev - total_spend


# ------------------------------------------------------------
# FIGURE
# ------------------------------------------------------------

    fig = plt.figure(figsize=(16, 20), facecolor=C["bg"])

    gs = GridSpec(
        5, 1,
        figure=fig,
        height_ratios=[0.8, 1.2, 2.5, 2.5, 3]
    )


# ------------------------------------------------------------
# HEADER
# ------------------------------------------------------------

    ax_header = fig.add_subplot(gs[0])
    ax_header.axis("off")

    date_range = f"{dates[0].strftime('%d %b')} – {dates[-1].strftime('%d %b %Y')}"

    ax_header.text(0.02, 0.7, title,
                   fontsize=22,
                   fontweight="bold")

    ax_header.text(0.02, 0.3,
                   f"{territory} • Weekly Report • {date_range}",
                   fontsize=11,
                   color=C["text_light"])


# ------------------------------------------------------------
# SCORECARDS
# ------------------------------------------------------------

    gs_cards = gs[1].subgridspec(1, 6)

    cards = [

        ("Clicks", format_number(total_clicks), None),
        ("Avg CPC", format_currency(avg_cpc), None),
        ("Avg CTR", format_pct(avg_ctr), None),
        ("Spend", format_currency(total_spend), None),
        ("Publisher Rev", format_currency(publisher_rev), None),
        ("Gross Profit", format_currency(gross_profit), None),
    ]

    for i, (l, v, s) in enumerate(cards):
        ax = fig.add_subplot(gs_cards[i])
        draw_scorecard(ax, l, v, s, C["chart_blue"])


# ------------------------------------------------------------
# CHARTS
# ------------------------------------------------------------

    gs_charts = gs[2].subgridspec(1, 2)

    x = np.arange(len(dates))

    clicks = [daily[d]["clicks"] for d in dates]
    cpc = [daily[d]["cpc"] for d in dates]
    ctr = [daily[d]["ctr"] for d in dates]

    spend = [daily[d]["spend"] for d in dates]
    pub = [daily[d]["revenue"] * 0.5 for d in dates]
    profit = [pub[i] - spend[i] for i in range(len(dates))]


# ------------------------------------------------------------
# AD PERFORMANCE
# ------------------------------------------------------------

    # ------------------------------------------------------------
# AD PERFORMANCE
# ------------------------------------------------------------

    ax = fig.add_subplot(gs_charts[0])

    # Bars → Clicks
    bars = ax.bar(x, clicks, color=C["chart_blue"], label="Clicks")

    # Line → CPC
    ax2 = ax.twinx()
    cpc_line, = ax2.plot(
        x,
        cpc,
        color=C["chart_red"],
        marker="o",
        linewidth=2.5,
        markersize=6,
        label="CPC"
    )

    # Title
    ax.set_title("Ad Performance")

    # Axis labels
    ax.set_ylabel("Clicks")
    ax2.set_ylabel("CPC (£)")

    # X labels
    ax.set_xticks(x)
    ax.set_xticklabels([d.strftime("%a\n%d") for d in dates])

    # Grid
    ax.grid(axis="y", color=C["grid"], linestyle="--", alpha=0.5)

    # ------------------------------------------------------------
    # CTR labels above bars
    # ------------------------------------------------------------

    for i, v in enumerate(ctr):

        ax.text(
            i,
            clicks[i] * 0.9,
            f"CTR: {v:.1f}%",
            ha="center",
            va="top",
            fontsize=9,
            color="black",
            bbox=dict(
                facecolor="white",
                edgecolor="none",
                alpha=0.7,
                boxstyle="round,pad=0.2"
            )
        )

    # ------------------------------------------------------------
    # Legend
    # ------------------------------------------------------------

    ax.legend([bars[0], cpc_line], ["Clicks", "CPC"], loc="upper left")

# ------------------------------------------------------------
# FINANCIAL PERFORMANCE
# ------------------------------------------------------------

    ax = fig.add_subplot(gs_charts[1])

    width = 0.35

    ax.bar(x - width/2, spend, width, color=C["chart_red"], label="Spend")
    ax.bar(x + width/2, pub, width, color=C["chart_green"], label="Publisher Rev")

    ax2 = ax.twinx()
    ax2.plot(x, profit, color=C["chart_gold"], marker="o", label="Profit")

    ax.set_title("Financial Performance")

    ax.set_ylabel("£ Spend / Revenue")
    ax2.set_ylabel("£ Profit")

    ax.set_xticks(x)
    ax.set_xticklabels([d.strftime("%a\n%d") for d in dates])

    ax.grid(axis="y", color=C["grid"])

    from matplotlib.lines import Line2D
    profit_line = Line2D([0], [0], color=C["chart_gold"], marker="o", label="Gross Profit")
    handles, labels = ax.get_legend_handles_labels()
    handles.append(profit_line)
    ax.legend(handles=handles, loc="upper left")


# ------------------------------------------------------------
# SALES UNITS
# ------------------------------------------------------------

    gs_sales = gs[3].subgridspec(1, 2)

    ebook = [daily[d]["ebook_units"] for d in dates]
    pb = [daily[d]["paperback_units"] for d in dates]

    ax = fig.add_subplot(gs_sales[0])

    ax.bar(x - 0.2, ebook, 0.4, label="Ebook")
    ax.bar(x + 0.2, pb, 0.4, label="Paperback")

    ax.set_title("Sales Units")

    ax.set_xticks(x)
    ax.set_xticklabels([d.strftime("%a\n%d") for d in dates])

    ax.set_title("Sales Units")
    ax.set_ylabel("Units Sold")
    ax.set_xlabel("Day")
    ax.legend()
    ax.grid(axis="y", color=C["grid"])


# ------------------------------------------------------------
# KENP
# ------------------------------------------------------------

    ax = fig.add_subplot(gs_sales[1])

    kenp = [daily[d]["kenp"] for d in dates]

    ax.bar(x, kenp, color=C["chart_gold"])

    ax.set_title("KENP")

    ax.set_xticks(x)
    ax.set_xticklabels([d.strftime("%a\n%d") for d in dates])

    ax.set_ylabel("Pages Read")
    ax.set_xlabel("Day")
    ax.grid(axis="y", color=C["grid"])


# ------------------------------------------------------------
# SAVE
# ------------------------------------------------------------

    safe = "".join(c for c in title if c.isalnum() or c in " _-")

    path = f"{OUTPUT_DIR}/{safe[:40]}_{edition_id}_{territory}.png"

    fig.savefig(path, dpi=150, bbox_inches="tight")

    plt.close()

    print("✓", path)

    return path


# ------------------------------------------------------------
# RUN ALL
# ------------------------------------------------------------

def generate_all_weekly_reports(date=None):

    df = fetch_weekly_data(date)

    if df.empty:
        print("No data")
        return []

    df["date_start"] = pd.to_datetime(df["date_start"]).dt.date

    end = df["date_start"].max()
    start = end - timedelta(days=6)

    dates = [start + timedelta(days=i) for i in range(7)]

    books = df[["Title", "Edition_ID"]].drop_duplicates()

    files = []

    for _, r in books.iterrows():

        title = r["Title"]
        eid = r["Edition_ID"]

        book = df[df["Edition_ID"] == eid]

        for t in sorted(book["Territory"].unique()):

            terr = book[book["Territory"] == t]

            f = generate_territory_report(title, eid, t, terr, dates)

            if f:
                files.append(f)

    print("Done:", len(files))

    return files


# ------------------------------------------------------------

if __name__ == "__main__":

    import sys

    date = sys.argv[1] if len(sys.argv) > 1 else None

    generate_all_weekly_reports(date)