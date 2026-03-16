# app.py
import os
import traceback
from flask import Flask, jsonify, request
from main import run_pipeline

app = Flask(__name__)

# Cloud Run uses /tmp for writable storage
os.environ["GENRE_OUTPUT_DIR"] = "/tmp/genre_reports"
os.environ["WEEKLY_OUTPUT_DIR"] = "/tmp/weekly_reports"


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "ads-pipeline"})


@app.route("/run", methods=["POST"])
def run():
    """Run the daily data pipeline only."""
    try:
        result = run_pipeline()
        return jsonify(result), 200
    except Exception as e:
        print(f"Pipeline failed:\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/scorecards", methods=["POST"])
def scorecards():
    """Generate genre analysis cards and send to Slack."""
    try:
        from genre_analysis import generate_all
        from slack_sender import send_reports

        date = request.json.get("date") if request.is_json else None
        files = generate_all(date)
        sent = send_reports("scorecard", files)

        return jsonify({
            "status": "success",
            "generated": len(files),
            "sent": sent,
        }), 200
    except Exception as e:
        print(f"Scorecards failed:\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/weekly", methods=["POST"])
def weekly():
    """Generate weekly reports and send to Slack."""
    try:
        from generate_reports import generate_all_weekly_reports
        from slack_sender import send_reports

        date = request.json.get("date") if request.is_json else None
        files = generate_all_weekly_reports(date)
        sent = send_reports("weekly", files)

        return jsonify({
            "status": "success",
            "generated": len(files),
            "sent": sent,
        }), 200
    except Exception as e:
        print(f"Weekly reports failed:\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/daily", methods=["POST"])
def daily():
    """Full daily flow: pipeline → genre scorecards → Slack.
    
    Delivery rules:
    - No reports on Saturday/Sunday
    - Tue-Fri: reports for books with spend yesterday
    - Monday: reports for books with spend on Fri/Sat/Sun
    - No report if campaign had no spend in lookback window
    """
    try:
        from datetime import date, timedelta

        today = date.today()
        weekday = today.weekday()  # 0=Mon, 5=Sat, 6=Sun

        # No reports on weekends
        if weekday in (5, 6):
            return jsonify({"status": "skipped", "reason": "weekend"}), 200

        # Step 1: Run pipeline (always runs Mon-Fri)
        result = run_pipeline()
        if result.get("status") == "no_data":
            return jsonify(result), 200

        # Step 2: Determine lookback dates for active campaigns
        if weekday == 0:  # Monday — check Fri + Sat + Sun
            lookback_days = [today - timedelta(days=i) for i in range(1, 4)]
        else:  # Tue-Fri — check yesterday only
            lookback_days = [today - timedelta(days=1)]

        # Step 3: Find books with spend in lookback window
        from bq import get_client
        client = get_client()
        date_list = ",".join([f"DATE('{d}')" for d in lookback_days])
        active_query = f"""
        SELECT DISTINCT Edition_ID
        FROM `marketing-489109.facebook_ads.ads_sales_analytics`
        WHERE date_start IN ({date_list}) AND spend > 0
        """
        active_df = client.query(active_query).to_dataframe()
        active_editions = active_df["Edition_ID"].tolist()

        if not active_editions:
            return jsonify({"status": "no_active_campaigns"}), 200

        # Step 4: Generate genre analysis cards and send to Slack
        from genre_analysis import generate_all
        from send_to_slack import send_reports

        files = generate_all()

        # Filter to only active editions
        active_files = []
        for f in files:
            for eid in active_editions:
                if f"_{eid}" in f:
                    active_files.append(f)
                    break

        sent = send_reports("scorecard", active_files)

        return jsonify({
            "status": "success",
            "pipeline_rows": result.get("rows", 0),
            "scorecards_generated": len(files),
            "scorecards_sent": sent,
        }), 200
    except Exception as e:
        print(f"Daily flow failed:\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)