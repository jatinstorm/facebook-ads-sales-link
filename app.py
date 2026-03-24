# app.py
import os
import traceback
from flask import Flask, jsonify, request
from main import run_pipeline

app = Flask(__name__)

# Cloud Run uses /tmp for writable storage
os.environ["GENRE_OUTPUT_DIR"] = "/tmp/genre_reports"
os.environ["WEEKLY_OUTPUT_DIR"] = "/tmp/weekly_reports"
os.environ["LAUNCH_OUTPUT_DIR"] = "/tmp/launch_reports"


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
    """Generate genre analysis cards and send to Slack. Runs daily."""
    try:
        from genre_analysis import generate_all
        from slack_sender import send_reports

        data = request.get_json(silent=True) or {}
        date = data.get("date")
        print("Starting scorecard generation...")
        print("Using date:", date)

        files = generate_all(date)
        print(f"Generated {len(files)} reports")

        print("Sending reports to Slack...")
        sent = send_reports("scorecard", files)

        print(f"Slack upload complete. Sent {sent} reports")

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
    """Generate weekly reports and send to Slack.
    Weekdays only. Monday catches up Fri/Sat/Sun.
    Only for books with spend in lookback window."""
    try:
        from datetime import date, timedelta
        from generate_reports import generate_all_weekly_reports
        from slack_sender import send_reports
        from bq import get_client

        today = date.today()
        weekday = today.weekday()

        # Skip weekends
        if weekday in (5, 6):
            return jsonify({"status": "skipped", "reason": "weekend"}), 200

        # Lookback: Monday checks Fri+Sat+Sun, otherwise yesterday
        if weekday == 0:
            lookback_days = [today - timedelta(days=i) for i in range(1, 4)]
        else:
            lookback_days = [today - timedelta(days=1)]

        # Find books with spend in lookback window
        client = get_client()
        date_list = ",".join([f"DATE('{d}')" for d in lookback_days])
        active_df = client.query(f"""
            SELECT DISTINCT Edition_ID
            FROM `marketing-489109.facebook_ads.ads_sales_analytics`
            WHERE date_start IN ({date_list}) AND spend > 0
        """).to_dataframe()
        active_editions = active_df["Edition_ID"].tolist()

        if not active_editions:
            return jsonify({"status": "no_active_campaigns"}), 200

        # Generate weekly reports
        data = request.get_json(silent=True) or {}
        date_arg = data.get("date")
        files = generate_all_weekly_reports(date_arg)

        # Filter to active editions only
        active_files = [f for f in files if any(f"_{eid}" in f or f"_{eid}_" in f for eid in active_editions)]

        sent = send_reports("weekly", active_files)

        return jsonify({
            "status": "success",
            "active_books": len(active_editions),
            "generated": len(active_files),
            "sent": sent,
        }), 200
    except Exception as e:
        print(f"Weekly reports failed:\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500
    


@app.route("/launch", methods=["POST"])
def launch():
    """Generate launch comparison scorecards and send to Slack.
    Books that just hit 30d, 90d, or 12m milestones get a scorecard.
    Optionally pass edition_id and/or milestone in JSON body."""
    try:
        from launch_comparison import generate_all
        from slack_sender import send_reports
 
        data = request.get_json(silent=True) or {}
        edition_id = data.get("edition_id")
        milestone = data.get("milestone")
 
        if edition_id is not None:
            edition_id = int(edition_id)
 
        print(f"Starting launch scorecards (edition={edition_id}, milestone={milestone})...")
 
        files = generate_all(edition_id=edition_id, milestone=milestone)
        print(f"Generated {len(files)} launch scorecards")
 
        sent = 0
        if files:
            print("Sending launch scorecards to Slack...")
            sent = send_reports("launch", files)
            print(f"Slack upload complete. Sent {sent} scorecards")
 
        return jsonify({
            "status": "success",
            "generated": len(files),
            "sent": sent,
        }), 200
 
    except Exception as e:
        print(f"Launch scorecards failed:\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500
        
@app.route("/series-dashboard", methods=["POST"])
def series_dashboard():
    """Regenerate series projection dashboard and upload to GCS."""
    try:
        from series_projection import run_pipeline
        
        print("Starting series projection pipeline...")
        run_pipeline()
        
        return jsonify({"status": "success", "url": "https://storage.googleapis.com/storm-series-dashboard/index.html"}), 200
    except Exception as e:
        print(f"Series dashboard failed:\n{traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500







if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)