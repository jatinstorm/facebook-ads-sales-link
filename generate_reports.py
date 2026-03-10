# generate_weekly_report.py
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
from matplotlib.gridspec import GridSpec
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta

try:
    from bq import get_client
except ImportError:
    get_client = None

OUTPUT_DIR = "weekly_reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Color palette ──────────────────────────────────────────────
C = {
    "bg": "#FEFCF8",
    "card": "#FFFFFF",
    "card_shadow": "#E8E4DD",
    "header_bg": "#1B1B2F",
    "accent": "#E94560",
    "accent2": "#0984E3",
    "accent3": "#00B894",
    "gold": "#F4C430",
    "text_dark": "#1B1B2F",
    "text_mid": "#555555",
    "text_light": "#999999",
    "positive": "#00B894",
    "negative": "#E94560",
    "chart_blue": "#0984E3",
    "chart_red": "#E94560",
    "chart_gold": "#F4C430",
    "chart_green": "#00B894",
    "grid": "#EEEEEE",
    "table_header": "#1B1B2F",
    "table_row1": "#FFFFFF",
    "table_row2": "#F7F5F0",
    "divider": "#E0DCD5",
}


def fetch_weekly_data(date=None):
    """Fetch last 7 days of data."""
    client = get_client()
    end_date = f"DATE('{date}')" if date else "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"

    query = f"""
    WITH date_range AS (
        SELECT DATE_SUB({end_date}, INTERVAL 6 DAY) AS start_date, {end_date} AS end_date
    )
    SELECT
        a.Title, a.Edition_ID, a.Territory, a.adset_name, a.date_start,
        SUM(a.spend) AS spend, SUM(a.clicks) AS clicks,
        AVG(a.cpc) AS cpc, AVG(a.ctr) AS ctr,
        SUM(a.impressions) AS impressions,
        SUM(a.ebook_units) AS ebook_units,
        SUM(a.paperback_units) AS paperback_units,
        SUM(a.kenp) AS kenp,
        SUM(a.ebook_revenue) AS ebook_revenue,
        SUM(a.paperback_revenue) AS paperback_revenue
    FROM `marketing-489109.facebook_ads.ads_sales_analytics` a, date_range d
    WHERE a.date_start BETWEEN d.start_date AND d.end_date
    GROUP BY a.Title, a.Edition_ID, a.Territory, a.adset_name, a.date_start
    ORDER BY a.Title, a.Territory, a.date_start
    """
    return client.query(query).to_dataframe()


def format_currency(val, symbol="£"):
    if abs(val) >= 1000:
        return f"{symbol}{val:,.0f}"
    elif abs(val) >= 1:
        return f"{symbol}{val:,.2f}"
    return f"{symbol}{val:.2f}"


def format_number(val):
    if val == int(val):
        return f"{int(val):,}"
    return f"{val:,.1f}"


def format_pct(val):
    return f"{val:.1f}%"


def draw_scorecard(ax, label, value, subtitle=None, color=None):
    """Draw a single scorecard box."""
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Card background
    card = mpatches.FancyBboxPatch(
        (0.02, 0.02), 0.96, 0.96,
        boxstyle="round,pad=0.04",
        facecolor=C["card"],
        edgecolor=C["divider"],
        linewidth=1.2,
    )
    ax.add_patch(card)

    # Top accent line
    ax.plot([0.1, 0.9], [0.92, 0.92], color=color or C["accent"], linewidth=3, solid_capstyle="round")

    # Label
    ax.text(0.5, 0.78, label, fontsize=8, color=C["text_light"],
            ha="center", va="center", fontfamily="sans-serif", fontweight="medium")

    # Value
    val_color = color if color else C["text_dark"]
    ax.text(0.5, 0.48, value, fontsize=18, color=val_color,
            ha="center", va="center", fontfamily="sans-serif", fontweight="bold")

    # Subtitle
    if subtitle:
        ax.text(0.5, 0.18, subtitle, fontsize=7, color=C["text_light"],
                ha="center", va="center", fontfamily="sans-serif")


def draw_chart(ax, dates, values1, values2, label1, label2, color1, color2, title, ylabel1, ylabel2=None, is_currency=False):
    """Draw a dual-axis bar + line chart."""
    ax.set_facecolor(C["bg"])

    x = np.arange(len(dates))
    width = 0.5

    # Bars
    bars = ax.bar(x, values1, width, color=color1, alpha=0.85, zorder=3, edgecolor="none")
    ax.set_ylabel(ylabel1, fontsize=8, color=C["text_mid"], fontfamily="sans-serif")
    ax.set_xticks(x)
    day_labels = [d.strftime("%a\n%d %b") for d in dates]
    ax.set_xticklabels(day_labels, fontsize=7, color=C["text_mid"], fontfamily="sans-serif")
    ax.tick_params(axis="y", labelsize=7, colors=C["text_mid"])

    # Grid
    ax.set_axisbelow(True)
    ax.grid(axis="y", color=C["grid"], linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False) if not ylabel2 else None
    ax.spines["left"].set_color(C["divider"])
    ax.spines["bottom"].set_color(C["divider"])

    # Line on secondary axis
    if values2 is not None:
        ax2 = ax.twinx()
        line = ax2.plot(x, values2, color=color2, linewidth=2.5, marker="o", markersize=5, zorder=4)
        ax2.set_ylabel(ylabel2 or label2, fontsize=8, color=C["text_mid"], fontfamily="sans-serif")
        ax2.tick_params(axis="y", labelsize=7, colors=C["text_mid"])
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_color(C["divider"])

    # Title
    ax.set_title(title, fontsize=11, fontweight="bold", color=C["text_dark"],
                 fontfamily="sans-serif", pad=12, loc="left")

    # Legend
    from matplotlib.lines import Line2D
    legend_elements = [
        mpatches.Patch(facecolor=color1, alpha=0.85, label=label1),
    ]
    if values2 is not None:
        legend_elements.append(Line2D([0], [0], color=color2, linewidth=2, label=label2))
    ax.legend(handles=legend_elements, fontsize=7, loc="upper right",
              frameon=True, facecolor=C["card"], edgecolor=C["divider"])


def draw_table(ax, dates, daily_data):
    """Draw the daily breakdown table."""
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    metrics = ["Clicks", "Ad Spend", "CPC", "CTR", "Ebook Units", "PB Units", "KENP", "Revenue"]
    n_cols = len(dates) + 2  # metric label + days + total
    n_rows = len(metrics) + 1  # header + metrics

    col_width = 0.85 / (len(dates) + 1)
    label_width = 0.15
    row_height = 1.0 / (n_rows + 0.5)
    start_y = 1.0 - row_height * 0.5

    # Header row
    ax.text(0.01, start_y, "Metric", fontsize=7, fontweight="bold", color=C["text_light"],
            va="center", fontfamily="sans-serif")
    for j, d in enumerate(dates):
        x = label_width + j * col_width + col_width / 2
        ax.text(x, start_y, d.strftime("%a %d"), fontsize=7, fontweight="bold",
                color=C["text_dark"], ha="center", va="center", fontfamily="sans-serif")
    # Total column
    x_total = label_width + len(dates) * col_width + col_width / 2
    ax.text(x_total, start_y, "Total", fontsize=7, fontweight="bold",
            color=C["accent"], ha="center", va="center", fontfamily="sans-serif")

    # Divider under header
    ax.plot([0.01, 0.99], [start_y - row_height * 0.45, start_y - row_height * 0.45],
            color=C["divider"], linewidth=1)

    for i, metric in enumerate(metrics):
        y = start_y - (i + 1) * row_height

        # Alternating row bg
        if i % 2 == 0:
            rect = mpatches.Rectangle((0, y - row_height * 0.4), 1, row_height * 0.8,
                                       facecolor=C["table_row2"], edgecolor="none")
            ax.add_patch(rect)

        # Metric label
        ax.text(0.01, y, metric, fontsize=7, color=C["text_mid"],
                va="center", fontfamily="sans-serif")

        total = 0
        for j, d in enumerate(dates):
            x = label_width + j * col_width + col_width / 2
            day_data = daily_data.get(d, {})

            if metric == "Clicks":
                val = day_data.get("clicks", 0)
                display = format_number(val)
                total += val
            elif metric == "Ad Spend":
                val = day_data.get("spend", 0)
                display = format_currency(val)
                total += val
            elif metric == "CPC":
                val = day_data.get("cpc", 0)
                display = format_currency(val)
            elif metric == "CTR":
                val = day_data.get("ctr", 0)
                display = format_pct(val)
            elif metric == "Ebook Units":
                val = day_data.get("ebook_units", 0)
                display = format_number(val)
                total += val
            elif metric == "PB Units":
                val = day_data.get("paperback_units", 0)
                display = format_number(val)
                total += val
            elif metric == "KENP":
                val = day_data.get("kenp", 0)
                display = format_number(val)
                total += val
            elif metric == "Revenue":
                val = day_data.get("revenue", 0)
                display = format_currency(val)
                total += val
            else:
                display = "—"

            ax.text(x, y, display, fontsize=7, color=C["text_dark"],
                    ha="center", va="center", fontfamily="sans-serif")

        # Total column
        if metric in ["CPC", "CTR"]:
            # Average for CPC/CTR
            vals = [daily_data.get(d, {}).get("cpc" if metric == "CPC" else "ctr", 0) for d in dates]
            valid = [v for v in vals if v > 0]
            avg = sum(valid) / len(valid) if valid else 0
            total_display = format_currency(avg) if metric == "CPC" else format_pct(avg)
        elif metric in ["Ad Spend", "Revenue"]:
            total_display = format_currency(total)
        else:
            total_display = format_number(total)

        ax.text(x_total, y, total_display, fontsize=7, fontweight="bold",
                color=C["accent"], ha="center", va="center", fontfamily="sans-serif")


def generate_territory_report(title, edition_id, territory, territory_data, dates):
    """Generate a full weekly report for one book + one territory."""

    # ── Aggregate daily data ──
    daily_data = {}
    for d in dates:
        day_df = territory_data[territory_data["date_start"] == d]
        if day_df.empty:
            daily_data[d] = {"spend": 0, "clicks": 0, "cpc": 0, "ctr": 0,
                             "impressions": 0, "ebook_units": 0, "paperback_units": 0,
                             "kenp": 0, "ebook_revenue": 0, "paperback_revenue": 0, "revenue": 0}
        else:
            spend = day_df["spend"].sum()
            clicks = day_df["clicks"].sum()
            impressions = day_df["impressions"].sum()
            ebook_rev = day_df["ebook_revenue"].sum()
            pb_rev = day_df["paperback_revenue"].sum()
            daily_data[d] = {
                "spend": spend,
                "clicks": clicks,
                "cpc": spend / clicks if clicks > 0 else 0,
                "ctr": (clicks / impressions * 100) if impressions > 0 else 0,
                "impressions": impressions,
                "ebook_units": day_df["ebook_units"].sum(),
                "paperback_units": day_df["paperback_units"].sum(),
                "kenp": day_df["kenp"].sum(),
                "ebook_revenue": ebook_rev,
                "paperback_revenue": pb_rev,
                "revenue": ebook_rev + pb_rev,
            }

    # ── Weekly totals ──
    total_clicks = sum(d["clicks"] for d in daily_data.values())
    total_spend = sum(d["spend"] for d in daily_data.values())
    total_impressions = sum(d["impressions"] for d in daily_data.values())
    avg_cpc = total_spend / total_clicks if total_clicks > 0 else 0
    avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    total_ebook = sum(d["ebook_units"] for d in daily_data.values())
    total_pb = sum(d["paperback_units"] for d in daily_data.values())
    total_kenp = sum(d["kenp"] for d in daily_data.values())
    total_revenue = sum(d["revenue"] for d in daily_data.values())
    publisher_rev = total_revenue * 0.5
    gross_profit = publisher_rev - total_spend
    best_cpc = min((d["cpc"] for d in daily_data.values() if d["cpc"] > 0), default=0)
    best_ctr = max((d["ctr"] for d in daily_data.values() if d["ctr"] > 0), default=0)
    avg_daily_spend = total_spend / len(dates) if dates else 0
    avg_daily_rev = total_revenue / len(dates) if dates else 0

    # ── Build the figure ──
    fig = plt.figure(figsize=(16, 20), facecolor=C["bg"])

    # Custom grid layout
    gs = GridSpec(5, 1, figure=fig, height_ratios=[0.8, 1.2, 2.5, 2.5, 3.0],
                  hspace=0.25, top=0.95, bottom=0.03, left=0.04, right=0.96)

    # ══════ ROW 0: Header ══════
    ax_header = fig.add_subplot(gs[0])
    ax_header.set_xlim(0, 1)
    ax_header.set_ylim(0, 1)
    ax_header.axis("off")

    # Dark header bar
    header_rect = mpatches.FancyBboxPatch(
        (0, 0), 1, 1,
        boxstyle="round,pad=0.02",
        facecolor=C["header_bg"],
        edgecolor="none",
    )
    ax_header.add_patch(header_rect)

    # Accent stripe
    ax_header.plot([0, 0.003, 0.003, 0], [0.1, 0.1, 0.9, 0.9],
                   color=C["accent"], linewidth=6, solid_capstyle="round")

    # Title
    display_title = title if len(title) < 40 else title[:37] + "..."
    ax_header.text(0.03, 0.65, display_title, fontsize=22, fontweight="bold",
                   color="white", fontfamily="sans-serif")

    # Subtitle
    date_range_str = f"{dates[0].strftime('%d %b')} – {dates[-1].strftime('%d %b %Y')}"
    ax_header.text(0.03, 0.25, f"{territory}  •  Weekly Report  •  {date_range_str}",
                   fontsize=11, color="#AAAAAA", fontfamily="sans-serif")

    # Territory badge
    badge_color = C["accent"] if territory == "GB" else C["accent2"]
    badge = mpatches.FancyBboxPatch(
        (0.88, 0.25), 0.1, 0.5,
        boxstyle="round,pad=0.02",
        facecolor=badge_color,
        edgecolor="none",
    )
    ax_header.add_patch(badge)
    ax_header.text(0.93, 0.5, territory, fontsize=16, fontweight="bold",
                   color="white", ha="center", va="center", fontfamily="sans-serif")

    # ══════ ROW 1: Scorecards ══════
    gs_cards = gs[1].subgridspec(1, 6, wspace=0.15)

    scorecards = [
        ("Total Clicks", format_number(total_clicks), f"Av. per day: {format_number(total_clicks/len(dates))}", C["chart_blue"]),
        ("Av. CPC", format_currency(avg_cpc), f"Best day: {format_currency(best_cpc)}", C["chart_blue"]),
        ("Av. CTR", format_pct(avg_ctr), f"Best day: {format_pct(best_ctr)}", C["chart_blue"]),
        ("Total Spend", format_currency(total_spend), f"Av. per day {format_currency(avg_daily_spend)}", C["chart_red"]),
        ("Publisher Rev", format_currency(publisher_rev), f"Av. per day {format_currency(avg_daily_rev * 0.5)}", C["chart_green"]),
        ("Gross Profit", format_currency(gross_profit), f"Revenue: {format_currency(total_revenue)}",
         C["positive"] if gross_profit >= 0 else C["negative"]),
    ]

    for i, (label, value, subtitle, color) in enumerate(scorecards):
        ax_card = fig.add_subplot(gs_cards[i])
        draw_scorecard(ax_card, label, value, subtitle, color)

    # ══════ ROW 2: Charts ══════
    gs_charts = gs[2].subgridspec(1, 2, wspace=0.25)

    # Chart 1: Clicks + CPC
    ax_chart1 = fig.add_subplot(gs_charts[0])
    clicks_series = [daily_data[d]["clicks"] for d in dates]
    cpc_series = [daily_data[d]["cpc"] for d in dates]
    draw_chart(ax_chart1, dates, clicks_series, cpc_series,
               "Clicks", "CPC", C["chart_blue"], C["chart_red"],
               "Ad Performance", "Clicks", "CPC (£)")

    # Chart 2: Revenue + Spend
    ax_chart2 = fig.add_subplot(gs_charts[1])
    rev_series = [daily_data[d]["revenue"] for d in dates]
    spend_series = [daily_data[d]["spend"] for d in dates]
    draw_chart(ax_chart2, dates, rev_series, spend_series,
               "Total Revenue", "Ad Spend", C["chart_green"], C["chart_red"],
               "Financial Performance", "Revenue (£)", "Spend (£)")

    # ══════ ROW 3: Units + KENP chart ══════
    gs_charts2 = gs[3].subgridspec(1, 2, wspace=0.25)

    ax_chart3 = fig.add_subplot(gs_charts2[0])
    ebook_series = [daily_data[d]["ebook_units"] for d in dates]
    pb_series = [daily_data[d]["paperback_units"] for d in dates]
    draw_chart(ax_chart3, dates, ebook_series, pb_series,
               "Ebook Units", "Paperback Units", C["chart_blue"], C["chart_gold"],
               "Sales Units", "Ebook Units", "PB Units")

    ax_chart4 = fig.add_subplot(gs_charts2[1])
    kenp_series = [daily_data[d]["kenp"] for d in dates]
    draw_chart(ax_chart4, dates, kenp_series, None,
               "KENP", None, C["chart_gold"], None,
               "Kindle Page Reads (KENP)", "KENP")

    # ══════ ROW 4: Daily breakdown table ══════
    ax_table = fig.add_subplot(gs[4])
    draw_table(ax_table, dates, daily_data)

    # Save
    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in title).strip()
    safe_title = safe_title.replace(" ", "_")[:40]
    filename = f"{OUTPUT_DIR}/{safe_title}_{edition_id}_{territory}.png"
    fig.savefig(filename, dpi=150, bbox_inches="tight", facecolor=C["bg"])
    plt.close(fig)

    print(f"  ✓ Saved: {filename}")
    return filename


def generate_all_weekly_reports(date=None):
    """Generate weekly reports for all active books."""
    print("Fetching weekly data...")
    df = fetch_weekly_data(date)

    if df.empty:
        print("No data found.")
        return []

    # Determine date range
    df["date_start"] = pd.to_datetime(df["date_start"]).dt.date
    end_date = df["date_start"].max()
    start_date = end_date - timedelta(days=6)
    dates = [start_date + timedelta(days=i) for i in range(7)]

    books = df[["Title", "Edition_ID"]].drop_duplicates()
    print(f"Found {len(books)} books, generating reports...\n")

    filenames = []
    for _, row in books.iterrows():
        title = row["Title"]
        eid = row["Edition_ID"]
        book_df = df[df["Edition_ID"] == eid]

        for territory in sorted(book_df["Territory"].unique()):
            terr_df = book_df[book_df["Territory"] == territory]
            print(f"  {title} ({territory})")
            fname = generate_territory_report(title, eid, territory, terr_df, dates)
            if fname:
                filenames.append(fname)

    print(f"\nDone! Generated {len(filenames)} reports in '{OUTPUT_DIR}/'")
    return filenames


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else None
    generate_all_weekly_reports(date)