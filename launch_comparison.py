# launch_comparison.py
"""
Book launch comparison with three time periods:
- First 30 days (from daily sales)
- First 90 days (from daily sales) — only if book is 90+ days old
- First 12 months (from monthly sales) — only if book is 12+ months old

Compares: This book vs All books avg vs Author avg vs Pub month cohort vs Genre avg
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

C = {
    "bg": "#FEFCF8",
    "card": "#FFFFFF",
    "header_bg": "#1B1B2F",
    "accent": "#E94560",
    "accent2": "#0984E3",
    "accent3": "#00B894",
    "gold": "#F4C430",
    "purple": "#8B5CF6",
    "text_dark": "#1B1B2F",
    "text_mid": "#555555",
    "text_light": "#999999",
    "positive": "#00B894",
    "negative": "#E94560",
    "divider": "#E0DCD5",
    "row_alt": "#F7F5F0",
    "this_book": "#E94560",
    "all_books": "#0984E3",
    "author": "#8B5CF6",
    "cohort": "#F59E0B",
    "genre": "#00B894",
    "period_30": "#E94560",
    "period_90": "#0984E3",
    "period_12m": "#8B5CF6",
}


def fetch_daily_launch_data(days=30):
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
        WHERE e.Format = 'Ebook' AND e.Pub_Date IS NOT NULL
        GROUP BY e.ID, e.Title, e.Cover_Author, e.Pub_Date, e.Genre, e.Genre_Subgenre
    ),
    ebook_sales AS (
        SELECT b.edition_id, b.title, b.author, b.pub_date, b.genre, b.genre_subgenre, b.pub_month,
            CASE WHEN s.Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS territory,
            SUM(IFNULL(s.Net_Units_Sold, 0)) AS ebook_units,
            SUM(IFNULL(s.Royalty_GBP, 0)) AS ebook_revenue
        FROM book_info b
        JOIN `storm-pub-amazon-sales.daily_sales.daily_sales_ebook_agg` s
            ON b.asin = s.ASIN
            AND s.Royalty_Date BETWEEN b.pub_date AND DATE_ADD(b.pub_date, INTERVAL {days - 1} DAY)
        GROUP BY b.edition_id, b.title, b.author, b.pub_date, b.genre, b.genre_subgenre, b.pub_month, territory
    ),
    pb_sales AS (
        SELECT b.edition_id,
            CASE WHEN s.Marketplace = 'Amazon.co.uk' THEN 'GB' ELSE 'US' END AS territory,
            SUM(IFNULL(s.Net_Units_Sold, 0)) AS pb_units,
            SUM(IFNULL(s.Royalty_GBP, 0)) AS pb_revenue
        FROM book_info b
        JOIN `storm-pub-amazon-sales.daily_sales.daily_sales_paperback_agg` s
            ON b.isbn = CAST(s.ISBN AS STRING)
            AND s.Royalty_Date BETWEEN b.pub_date AND DATE_ADD(b.pub_date, INTERVAL {days - 1} DAY)
        WHERE b.isbn IS NOT NULL
        GROUP BY b.edition_id, territory
    )
    SELECT
        e.edition_id, e.title, e.author, e.pub_date, e.genre, e.genre_subgenre, e.pub_month, e.territory,
        e.ebook_units + IFNULL(p.pb_units, 0) AS units,
        e.ebook_revenue + IFNULL(p.pb_revenue, 0) AS revenue
    FROM ebook_sales e
    LEFT JOIN pb_sales p ON e.edition_id = p.edition_id AND e.territory = p.territory
    """

    return client.query(query).to_dataframe()

   


def fetch_daily_kenp_data(days=30):
    """Fetch first N days KENP from daily kenp table."""
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
        WHERE e.Format = 'Ebook' AND e.Pub_Date IS NOT NULL
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
        AND k.Date BETWEEN b.pub_date AND DATE_ADD(b.pub_date, INTERVAL {days - 1} DAY)
    GROUP BY b.edition_id, territory
    """

    return client.query(query).to_dataframe()


def fetch_monthly_launch_data():
    """Fetch first 12 months from monthly sales."""
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
        SUM(CASE WHEN m.payout_plan IN ('Standard', 'Pre Order') THEN IFNULL(m.net_units_sold, 0) ELSE 0 END) AS units,
        SUM(IFNULL(m.gbp_revenue, 0)) AS revenue,
        SUM(CASE WHEN m.payout_plan = 'Kindle Edition Normalized Pages (KENP)' THEN IFNULL(m.ku_books_read, 0) ELSE 0 END) AS kenp
    FROM `storm-pub-amazon-sales.airtable.awe_editions` e
    JOIN `storm-pub-amazon-sales.monthly_sales.monthly_sales` m
        ON e.ID = m.edition_id
        AND m.date BETWEEN DATE_TRUNC(e.Pub_Date, MONTH) AND DATE_ADD(DATE_TRUNC(e.Pub_Date, MONTH), INTERVAL 11 MONTH)
    WHERE e.Format = 'Ebook' AND e.Pub_Date IS NOT NULL
    GROUP BY e.ID, e.Title, e.Cover_Author, e.Pub_Date, e.Genre, e.Genre_Subgenre, pub_month, territory
    """

    return client.query(query).to_dataframe()


def avg_per_book(df):
    """Average metrics per book from grouped data."""
    if df.empty:
        return {"units": 0, "revenue": 0, "kenp": 0, "books": 0}
    grouped = df.groupby("edition_id").agg({
        "units": "sum", "revenue": "sum", "kenp": "sum"
    }).reset_index()
    n = len(grouped)
    return {
            "units": grouped["units"].median() if n > 0 else 0,
            "revenue": grouped["revenue"].median() if n > 0 else 0,
            "kenp": grouped["kenp"].median() if n > 0 else 0,
            "books": n,
        }


def aggregate(df):
    return {
        "units": df["units"].sum() if not df.empty else 0,
        "revenue": df["revenue"].sum() if not df.empty else 0,
        "kenp": df["kenp"].sum() if not df.empty else 0,
    }


def fc(val):
    if abs(val) >= 1000:
        return f"£{val:,.0f}"
    return f"£{val:,.2f}"


def fn(val):
    if isinstance(val, float):
        if val == int(val):
            return f"{int(val):,}"
        return f"{val:,.1f}"
    return f"{int(val):,}"


def pct_vs(book_val, avg_val):
    if avg_val == 0:
        return "--", C["text_light"]
    diff = ((book_val - avg_val) / avg_val) * 100
    color = C["positive"] if diff > 10 else C["negative"] if diff < -10 else C["text_light"]
    sign = "+" if diff > 0 else ""
    return f"{sign}{diff:.0f}%", color


def generate_book_launch_card(title, edition_id, author, pub_date, genre, subgenre,
                               periods_data):
    """
    Generate a launch card with multiple time periods.
    periods_data = {
        "30d": {"book": {terr: metrics}, "all": {terr: metrics}, "author": ..., "cohort": ..., "genre": ...},
        "90d": {...},  # or None if not enough data
        "12m": {...},  # or None if not enough data
    }
    """

    available_periods = [(k, v) for k, v in periods_data.items() if v is not None]
    if not available_periods:
        return None

    # Collect all territories across all periods
    all_territories = set()
    for _, pdata in available_periods:
        all_territories.update(pdata["book"].keys())
    territories = sorted(all_territories)

    if not territories:
        return None

    # Layout
    row_h = 0.4
    header_h = 1.8
    label_w = 3.5
    col_w = 2.2
    n_cols = 5  # This Book, All Books, Author, Cohort, Genre
    comp_labels_base = ["This Book", "All Books Avg", "Author Avg", "Pub Month Cohort", "Genre Avg"]
    comp_colors = [C["this_book"], C["all_books"], C["author"], C["cohort"], C["genre"]]

    metrics = ["Units Sold", "Revenue", "KENP"]
    period_labels = {"30d": "First 30 Days", "90d": "First 90 Days", "12m": "First 12 Months"}
    period_colors = {"30d": C["period_30"], "90d": C["period_90"], "12m": C["period_12m"]}

    # Calculate total rows
    total_rows = 0
    for period_key, _ in available_periods:
        total_rows += 1  # period header
        total_rows += len(metrics) * len(territories) + len(territories)  # metrics + territory headers

    fig_h = header_h + (total_rows * row_h) + 2.0
    fig_w = label_w + (n_cols * col_w) + 0.6

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor(C["bg"])
    ax.set_facecolor(C["bg"])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.axis("off")

    # ── Header ──
    header = mpatches.FancyBboxPatch(
        (0.2, fig_h - header_h), fig_w - 0.4, header_h - 0.1,
        boxstyle="round,pad=0.1",
        facecolor=C["header_bg"], edgecolor=C["accent"], linewidth=2.5,
    )
    ax.add_patch(header)

    display_title = title if len(title) < 45 else title[:42] + "..."
    ax.text(0.5, fig_h - 0.5, display_title,
            fontsize=18, fontweight="bold", color="white", fontfamily="sans-serif")

    pub_date_obj = pub_date if isinstance(pub_date, date) else pd.to_datetime(pub_date).date()
    days_since = (date.today() - pub_date_obj).days
    ax.text(0.5, fig_h - 0.9,
            f"by {author}  •  Published {pub_date_obj.strftime('%d %b %Y')}  •  {days_since} days ago",
            fontsize=10, color="#AAAAAA", fontfamily="sans-serif")

    genre_display = subgenre if subgenre and str(subgenre) != "nan" else genre
    ax.text(0.5, fig_h - 1.25,
            f"Genre: {genre_display}  •  Edition {edition_id}",
            fontsize=9, color="#888888", fontfamily="sans-serif")

    # ── Column headers ──
    y_start = fig_h - header_h - 0.2

    for j, (lbl, clr) in enumerate(zip(comp_labels_base, comp_colors)):
        x = label_w + (j * col_w) + (col_w / 2)
        # Get book count from first available period
        first_period = available_periods[0][1]
        count = ""
        if j == 1:
            count = f"\n({first_period['all'].get('books', 0)})"
        elif j == 2:
            count = f"\n({first_period['author'].get('books', 0)} books)"
        elif j == 3:
            count = f"\n({first_period['cohort'].get('books', 0)})"
        elif j == 4:
            count = f"\n({first_period['genre'].get('books', 0)})"

        pill = mpatches.FancyBboxPatch(
            (x - col_w / 2 + 0.05, y_start - 0.45), col_w - 0.1, 0.5,
            boxstyle="round,pad=0.04",
            facecolor=clr + "22", edgecolor=clr, linewidth=1,
        )
        ax.add_patch(pill)
        ax.text(x, y_start - 0.2, f"{lbl}{count}",
                fontsize=7, fontweight="bold", color=clr,
                ha="center", va="center", fontfamily="sans-serif")

    # ── Period sections ──
    y = y_start - 0.7
    metric_keys = ["units", "revenue", "kenp"]
    metric_labels = ["Units Sold", "Revenue", "KENP"]
    metric_fmts = [fn, fc, fn]

    for period_key, pdata in available_periods:
        # Period header
        p_color = period_colors[period_key]
        p_label = period_labels[period_key]

        period_rect = mpatches.FancyBboxPatch(
            (0.2, y - 0.12), fig_w - 0.4, 0.35,
            boxstyle="round,pad=0.04",
            facecolor=p_color + "15", edgecolor=p_color, linewidth=1.5,
        )
        ax.add_patch(period_rect)
        ax.text(fig_w / 2, y + 0.05, p_label,
                fontsize=10, fontweight="bold", color=p_color,
                ha="center", va="center", fontfamily="sans-serif")
        y -= row_h

        for terr in territories:
            # Territory sub-header
            terr_color = C["this_book"] if terr == "GB" else C["accent2"]
            ax.text(0.45, y + 0.05, terr,
                    fontsize=9, fontweight="bold", color=terr_color,
                    va="center", fontfamily="sans-serif")
            y -= row_h * 0.6

            book_m = pdata["book"].get(terr, {"units": 0, "revenue": 0, "kenp": 0})
            all_m = pdata["all"].get(terr, {"units": 0, "revenue": 0, "kenp": 0})
            auth_m = pdata["author"].get(terr, {"units": 0, "revenue": 0, "kenp": 0})
            coh_m = pdata["cohort"].get(terr, {"units": 0, "revenue": 0, "kenp": 0})
            gen_m = pdata["genre"].get(terr, {"units": 0, "revenue": 0, "kenp": 0})

            for i, (key, label, fmt) in enumerate(zip(metric_keys, metric_labels, metric_fmts)):
                row_color = C["row_alt"] if i % 2 == 0 else C["card"]
                rect = mpatches.Rectangle(
                    (0.2, y - 0.15), fig_w - 0.4, row_h,
                    facecolor=row_color, edgecolor="none",
                )
                ax.add_patch(rect)

                ax.text(0.65, y + 0.05, label,
                        fontsize=8, color=C["text_mid"], va="center", fontfamily="sans-serif")

                vals = [book_m.get(key, 0), all_m.get(key, 0), auth_m.get(key, 0),
                        coh_m.get(key, 0), gen_m.get(key, 0)]

                for j, (val, clr) in enumerate(zip(vals, comp_colors)):
                    x = label_w + (j * col_w) + (col_w / 2)
                    ax.text(x, y + 0.1, fmt(val),
                            fontsize=9, fontweight="bold", color=C["text_dark"],
                            ha="center", va="center", fontfamily="sans-serif")

                    if j > 0 and vals[0] > 0:
                        pct_text, pct_color = pct_vs(vals[0], val)
                        ax.text(x, y - 0.06, pct_text,
                                fontsize=7, color=pct_color,
                                ha="center", va="center", fontfamily="sans-serif")

                y -= row_h

        y -= 0.1  # Gap between periods

    # ── Bottom line ──
    ax.plot([0.3, fig_w - 0.3], [0.15, 0.15], color=C["accent"], linewidth=2)

    plt.tight_layout(pad=0.3)

    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in title).strip()
    safe_title = safe_title.replace(" ", "_")[:40]
    filename = f"{OUTPUT_DIR}/{safe_title}_{edition_id}.png"
    fig.savefig(filename, dpi=150, bbox_inches="tight", facecolor=C["bg"])
    plt.close(fig)

    print(f"  ✓ {filename}")
    return filename


def build_comparisons(df, eid, author, genre, pub_month, territories):
    """Build comparison dict for one time period."""
    book_df = df[df["edition_id"] == eid]
    book = {}
    for terr in territories:
        t = book_df[book_df["territory"] == terr]
        book[terr] = aggregate(t)

    all_avg = {"books": df["edition_id"].nunique()}
    author_df = df[(df["author"] == author) & (df["edition_id"] != eid)]
    author_avg = {"books": author_df["edition_id"].nunique()}
    cohort_df = df[(df["pub_month"] == pub_month) & (df["edition_id"] != eid)]
    cohort_avg = {"books": cohort_df["edition_id"].nunique()}
    genre_df = df[(df["genre"] == genre) & (df["edition_id"] != eid)]
    genre_avg = {"books": genre_df["edition_id"].nunique()}

    for terr in ["GB", "US"]:
        all_avg[terr] = avg_per_book(df[df["territory"] == terr])
        author_avg[terr] = avg_per_book(author_df[author_df["territory"] == terr])
        cohort_avg[terr] = avg_per_book(cohort_df[cohort_df["territory"] == terr])
        genre_avg[terr] = avg_per_book(genre_df[genre_df["territory"] == terr])

    return {
        "book": book,
        "all": all_avg,
        "author": author_avg,
        "cohort": cohort_avg,
        "genre": genre_avg,
    }


def merge_kenp(daily_df, kenp_df):
    """Merge KENP data into daily sales dataframe."""
    if kenp_df.empty:
        daily_df["kenp"] = 0
        return daily_df

    kenp_grouped = kenp_df.groupby(["edition_id", "territory"]).agg({
        "kenp": "sum"
    }).reset_index()

    merged = daily_df.merge(kenp_grouped, on=["edition_id", "territory"], how="left")
    merged["kenp"] = merged["kenp"].fillna(0)
    return merged


def generate_all(pub_month=None, edition_id=None):
    """Generate launch comparisons for books published in a given month."""
    print("Fetching launch data...")

    # Fetch 30-day data
    print("  Fetching 30-day daily sales...")
    daily_30 = fetch_daily_launch_data(30)
    kenp_30 = fetch_daily_kenp_data(30)
    daily_30 = merge_kenp(daily_30, kenp_30)

    # Fetch 90-day data
    print("  Fetching 90-day daily sales...")
    daily_90 = fetch_daily_launch_data(90)
    kenp_90 = fetch_daily_kenp_data(90)
    daily_90 = merge_kenp(daily_90, kenp_90)

    # Fetch 12-month data
    print("  Fetching 12-month monthly sales...")
    monthly_12 = fetch_monthly_launch_data()

    if daily_30.empty:
        print("No data found.")
        return []

# Target books: published in last 30 days
    daily_30["pub_date"] = pd.to_datetime(daily_30["pub_date"]).dt.date

    if edition_id:
        print(f"Generating report for edition {edition_id}\n")

        target_books = daily_30[daily_30["edition_id"] == edition_id][
            ["edition_id", "title", "author", "pub_date", "genre", "genre_subgenre", "pub_month"]
        ].drop_duplicates(subset=["edition_id"])

    else:
        today = date.today()
        cutoff = today - timedelta(days=30)

        print(f"Books published in last 30 days (since {cutoff}):\n")

        target_books = daily_30[daily_30["pub_date"] >= cutoff][
            ["edition_id", "title", "author", "pub_date", "genre", "genre_subgenre", "pub_month"]
        ].drop_duplicates(subset=["edition_id"])

    if target_books.empty:
        print(f"No books published in {target_month}")
        return []

    print(f"Found {len(target_books)} books\n")
    today = date.today()

    filenames = []
    for _, row in target_books.iterrows():
        eid = row["edition_id"]
        title = row["title"]
        author = row["author"]
        pub_date = row["pub_date"]
        genre = row["genre"]
        subgenre = row["genre_subgenre"]
        pub_m = row["pub_month"]

        days_since = (today - pub_date).days

        periods = {}

        # 30-day (always show — even partial)
        if not daily_30[daily_30["edition_id"] == eid].empty:
            label = "30d"
            periods[label] = build_comparisons(daily_30, eid, author, genre, pub_m, ["GB", "US"])

        # 90-day (only if 90+ days old)
        if days_since >= 90 and not daily_90[daily_90["edition_id"] == eid].empty:
            periods["90d"] = build_comparisons(daily_90, eid, author, genre, pub_m, ["GB", "US"])

        # 12-month (only if 365+ days old)
        if days_since >= 365 and not monthly_12.empty and not monthly_12[monthly_12["edition_id"] == eid].empty:
            periods["12m"] = build_comparisons(monthly_12, eid, author, genre, pub_m, ["GB", "US"])

        if not periods:
            continue

        print(f"📖 {title} by {author} ({days_since}d old, periods: {list(periods.keys())})")
        fname = generate_book_launch_card(
            title, eid, author, pub_date, genre, subgenre, periods
        )
        if fname:
            filenames.append(fname)

    print(f"\nDone! {len(filenames)} reports in '{OUTPUT_DIR}/'")
    return filenames


if __name__ == "__main__":
    import sys

    edition_id = None
    pub_month = None

    if len(sys.argv) > 1:
        try:
            edition_id = int(sys.argv[1])
        except:
            pub_month = sys.argv[1]

    generate_all(pub_month=pub_month, edition_id=edition_id)