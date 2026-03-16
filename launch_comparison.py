# generate_launch_comparison.py
"""
Book launch comparison:
- This book's first month performance
- vs all books' first month average
- vs same author's first month average
- vs same pub-month cohort (books published same month)
- vs same genre first month average
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

OUTPUT_DIR = "launch_reports"
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
}


def fetch_launch_data(target_edition_id=None):
    """Fetch first-month performance for all books."""
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
        AND DATE_TRUNC(e.Pub_Date, MONTH) = m.date
    WHERE e.Format = 'Ebook'
        AND e.Pub_Date IS NOT NULL
    GROUP BY e.ID, e.Title, e.Cover_Author, e.Pub_Date, e.Genre, e.Genre_Subgenre, pub_month, territory
    """

    df = client.query(query).to_dataframe()
    return df


def aggregate(df):
    """Aggregate metrics from a filtered dataframe."""
    return {
        "units": df["units"].sum(),
        "revenue": df["revenue"].sum(),
        "kenp": df["kenp"].sum(),
        "books": df["edition_id"].nunique() if "edition_id" in df.columns else 0,
    }


def avg_per_book(df):
    """Calculate average per book from grouped data."""
    if df.empty:
        return {"units": 0, "revenue": 0, "kenp": 0, "books": 0}
    grouped = df.groupby("edition_id").agg({
        "units": "sum", "revenue": "sum", "kenp": "sum"
    }).reset_index()
    n = len(grouped)
    return {
        "units": grouped["units"].mean() if n > 0 else 0,
        "revenue": grouped["revenue"].mean() if n > 0 else 0,
        "kenp": grouped["kenp"].mean() if n > 0 else 0,
        "books": n,
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
    """Return percentage difference and color."""
    if avg_val == 0:
        return "--", C["text_light"]
    diff = ((book_val - avg_val) / avg_val) * 100
    color = C["positive"] if diff > 10 else C["negative"] if diff < -10 else C["text_light"]
    sign = "+" if diff > 0 else ""
    return f"{sign}{diff:.0f}%", color


def generate_book_launch_card(title, edition_id, author, pub_date, genre, subgenre,
                               book_data, all_avg, author_avg, cohort_avg, genre_avg):
    """Generate a launch comparison card."""

    territories = sorted(book_data.keys())
    if not territories:
        return None

    n_terr = len(territories)

    # Layout
    row_h = 0.4
    header_h = 1.8
    label_w = 3.5
    col_w = 2.2
    n_cols = 5  # This Book, All Books Avg, Author Avg, Cohort Avg, Genre Avg
    fig_w = label_w + (n_cols * col_w * n_terr) + 1.0
    # Keep it simpler — one territory at a time, stack vertically
    metrics = ["Units Sold", "Revenue", "KENP"]
    comparisons = ["This Book", "All Books Avg", "Author Avg", "Pub Month Cohort", "Genre Avg"]
    
    total_rows = len(metrics) * n_terr + n_terr + 2  # metrics + territory headers + padding
    fig_h = header_h + (total_rows * row_h) + 2.5
    fig_w = label_w + (len(comparisons) * col_w) + 0.6

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

    # Title
    display_title = title if len(title) < 45 else title[:42] + "..."
    ax.text(0.5, fig_h - 0.5, display_title,
            fontsize=18, fontweight="bold", color="white", fontfamily="sans-serif")

    # Author + pub date + genre
    ax.text(0.5, fig_h - 0.9,
            f"by {author}  •  Published {pub_date.strftime('%d %b %Y')}",
            fontsize=10, color="#AAAAAA", fontfamily="sans-serif")

    genre_display = subgenre if subgenre and str(subgenre) != "nan" else genre
    ax.text(0.5, fig_h - 1.25,
            f"Genre: {genre_display}  •  Edition {edition_id}  •  First Month Performance",
            fontsize=9, color="#888888", fontfamily="sans-serif")

    # Comparison column headers
    y_start = fig_h - header_h - 0.2
    comp_colors = [C["this_book"], C["all_books"], C["author"], C["cohort"], C["genre"]]
    comp_labels = ["This Book", f"All Books\nAvg ({all_avg.get('books', 0)})", 
                   f"Author Avg\n({author_avg.get('books', 0)} books)",
                   f"Pub Month\nCohort ({cohort_avg.get('books', 0)})",
                   f"Genre Avg\n({genre_avg.get('books', 0)})"]

    for j, (lbl, clr) in enumerate(zip(comp_labels, comp_colors)):
        x = label_w + (j * col_w) + (col_w / 2)
        pill = mpatches.FancyBboxPatch(
            (x - col_w / 2 + 0.05, y_start - 0.45), col_w - 0.1, 0.5,
            boxstyle="round,pad=0.04",
            facecolor=clr + "22", edgecolor=clr, linewidth=1,
        )
        ax.add_patch(pill)
        ax.text(x, y_start - 0.2, lbl,
                fontsize=7, fontweight="bold", color=clr,
                ha="center", va="center", fontfamily="sans-serif")

    # ── Data rows per territory ──
    y = y_start - 0.7

    metric_keys = ["units", "revenue", "kenp"]
    metric_labels = ["Units Sold", "Revenue (£)", "KENP Pages"]
    metric_fmts = [fn, fc, fn]

    for terr in territories:
        # Territory header
        terr_color = C["this_book"] if terr == "GB" else C["accent2"]
        terr_rect = mpatches.FancyBboxPatch(
            (0.2, y - 0.12), fig_w - 0.4, 0.35,
            boxstyle="round,pad=0.04",
            facecolor=terr_color, edgecolor="none",
        )
        ax.add_patch(terr_rect)
        ax.text(0.5, y + 0.05, terr,
                fontsize=10, fontweight="bold", color="white",
                fontfamily="sans-serif")
        y -= row_h

        book_m = book_data.get(terr, {"units": 0, "revenue": 0, "kenp": 0})
        all_m = all_avg.get(terr, {"units": 0, "revenue": 0, "kenp": 0})
        auth_m = author_avg.get(terr, {"units": 0, "revenue": 0, "kenp": 0})
        coh_m = cohort_avg.get(terr, {"units": 0, "revenue": 0, "kenp": 0})
        gen_m = genre_avg.get(terr, {"units": 0, "revenue": 0, "kenp": 0})

        for i, (key, label, fmt) in enumerate(zip(metric_keys, metric_labels, metric_fmts)):
            row_color = C["row_alt"] if i % 2 == 0 else C["card"]
            rect = mpatches.Rectangle(
                (0.2, y - 0.15), fig_w - 0.4, row_h,
                facecolor=row_color, edgecolor="none",
            )
            ax.add_patch(rect)

            # Metric label
            ax.text(0.45, y + 0.05, label,
                    fontsize=9, color=C["text_mid"], va="center", fontfamily="sans-serif")

            # Values for each comparison
            vals = [book_m.get(key, 0), all_m.get(key, 0), auth_m.get(key, 0),
                    coh_m.get(key, 0), gen_m.get(key, 0)]

            for j, (val, clr) in enumerate(zip(vals, comp_colors)):
                x = label_w + (j * col_w) + (col_w / 2)

                # Main value
                ax.text(x, y + 0.1, fmt(val),
                        fontsize=10, fontweight="bold", color=C["text_dark"],
                        ha="center", va="center", fontfamily="sans-serif")

                # Percentage comparison (skip for "This Book" column)
                if j > 0 and vals[0] > 0:
                    pct_text, pct_color = pct_vs(vals[0], val)
                    ax.text(x, y - 0.08, pct_text,
                            fontsize=7, color=pct_color,
                            ha="center", va="center", fontfamily="sans-serif")

            y -= row_h

        y -= 0.15  # Gap between territories

    # ── Bottom accent line ──
    ax.plot([0.3, fig_w - 0.3], [0.15, 0.15], color=C["accent"], linewidth=2)

    plt.tight_layout(pad=0.3)

    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in title).strip()
    safe_title = safe_title.replace(" ", "_")[:40]
    filename = f"{OUTPUT_DIR}/{safe_title}_{edition_id}.png"
    fig.savefig(filename, dpi=150, bbox_inches="tight", facecolor=C["bg"])
    plt.close(fig)

    print(f"  ✓ {filename}")
    return filename


def generate_all(pub_month=None):
    """
    Generate launch comparisons.
    pub_month: e.g. '2026-01' to compare all books published that month.
    If None, uses the latest month with published books.
    """
    print("Fetching launch data...")
    df = fetch_launch_data()

    if df.empty:
        print("No data found.")
        return []

    df["pub_date"] = pd.to_datetime(df["pub_date"]).dt.date

    # Determine target month
    if pub_month:
        target_month = pub_month
    else:
        target_month = df["pub_month"].max()

    print(f"Comparing books published in: {target_month}\n")

    # Target books (published in target month)
    target_books = df[df["pub_month"] == target_month][
        ["edition_id", "title", "author", "pub_date", "genre", "genre_subgenre"]
    ].drop_duplicates(subset=["edition_id"])

    if target_books.empty:
        print(f"No books published in {target_month}")
        return []

    print(f"Found {len(target_books)} books\n")

    filenames = []
    for _, row in target_books.iterrows():
        eid = row["edition_id"]
        title = row["title"]
        author = row["author"]
        pub_date = row["pub_date"]
        genre = row["genre"]
        subgenre = row["genre_subgenre"]

        # This book's data per territory
        book_df = df[df["edition_id"] == eid]
        book_data = {}
        for terr in book_df["territory"].unique():
            t = book_df[book_df["territory"] == terr]
            book_data[terr] = aggregate(t)

        # All books average per territory
        all_avg = {}
        for terr in ["GB", "US"]:
            t = df[df["territory"] == terr]
            all_avg[terr] = avg_per_book(t)
        all_avg["books"] = df["edition_id"].nunique()

        # Same author average per territory
        author_df = df[(df["author"] == author) & (df["edition_id"] != eid)]
        author_avg = {}
        for terr in ["GB", "US"]:
            t = author_df[author_df["territory"] == terr]
            author_avg[terr] = avg_per_book(t)
        author_avg["books"] = author_df["edition_id"].nunique()

        # Same pub month cohort per territory
        cohort_df = df[(df["pub_month"] == target_month) & (df["edition_id"] != eid)]
        cohort_avg = {}
        for terr in ["GB", "US"]:
            t = cohort_df[cohort_df["territory"] == terr]
            cohort_avg[terr] = avg_per_book(t)
        cohort_avg["books"] = cohort_df["edition_id"].nunique()

        # Same genre average per territory
        genre_df = df[(df["genre"] == genre) & (df["edition_id"] != eid)]
        genre_avg = {}
        for terr in ["GB", "US"]:
            t = genre_df[genre_df["territory"] == terr]
            genre_avg[terr] = avg_per_book(t)
        genre_avg["books"] = genre_df["edition_id"].nunique()

        print(f"📖 {title} by {author}")
        fname = generate_book_launch_card(
            title, eid, author, pub_date, genre, subgenre,
            book_data, all_avg, author_avg, cohort_avg, genre_avg
        )
        if fname:
            filenames.append(fname)

    print(f"\nDone! {len(filenames)} reports in '{OUTPUT_DIR}/'")
    return filenames


if __name__ == "__main__":
    import sys
    pub_month = sys.argv[1] if len(sys.argv) > 1 else None
    generate_all(pub_month)