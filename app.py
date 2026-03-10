# app.py
import os
import traceback
from flask import Flask, jsonify, request
from main import run_pipeline

app = Flask(__name__)


@app.route("/", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "service": "ads-pipeline"})


@app.route("/run", methods=["POST"])
def run():
    """
    Trigger the daily pipeline.
    Called by Cloud Scheduler every morning.
    """
    try:
        result = run_pipeline()
        return jsonify(result), 200
    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"Pipeline failed:\n{error_msg}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)