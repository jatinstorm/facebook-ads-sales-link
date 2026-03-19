# send_to_slack.py
"""
Send generated reports to author Slack channels.
Uploads to GCS first, sends to Slack, then cleans up GCS.

Requires env vars: SLACK_BOT_TOKEN, GCS_BUCKET
"""
import os
import tempfile
import requests
import time
from bq import get_client

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_BASE_URL = "https://slack.com/api"

_channel_cache = {}


def _headers():
    return {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}


def lookup_channel_id(channel_name):
    """Resolve #channel-name to a Slack channel ID."""
    name = channel_name.lstrip("#")

    if name in _channel_cache:
        return _channel_cache[name]

    cursor = None
    while True:
        params = {"types": "public_channel,private_channel", "limit": 200}
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(
            f"{SLACK_BASE_URL}/conversations.list",
            headers=_headers(),
            params=params,
            timeout=30,
        )
        data = resp.json()

        if not data.get("ok"):
            print(f"  ✗ Slack API error: {data.get('error')}")
            return None

        for ch in data.get("channels", []):
            _channel_cache[ch["name"]] = ch["id"]
            if ch["name"] == name:
                return ch["id"]

        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    print(f"  ✗ Channel not found: #{name}")
    return None


def upload_file_to_slack(channel_id, filepath, title, message=None):
    """Upload a local file to a Slack channel."""
    if not SLACK_BOT_TOKEN:
        print("  ✗ SLACK_BOT_TOKEN not set")
        return False

    filesize = os.path.getsize(filepath)
    resp = requests.post(
        f"{SLACK_BASE_URL}/files.getUploadURLExternal",
        headers=_headers(),
        data={"filename": os.path.basename(filepath), "length": filesize},
        timeout=30,
    )
    data = resp.json()
    if not data.get("ok"):
        print(f"  ✗ Upload URL failed: {data.get('error')}")
        return False

    upload_url = data["upload_url"]
    file_id = data["file_id"]

    with open(filepath, "rb") as f:
        requests.post(upload_url, files={"file": f}, timeout=60)

    body = {
        "files": [{"id": file_id, "title": title}],
        "channel_id": channel_id,
    }
    if message:
        body["initial_comment"] = message

    resp = requests.post(
        f"{SLACK_BASE_URL}/files.completeUploadExternal",
        headers={**_headers(), "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    result = resp.json()
    if result.get("ok"):
        return True
    else:
        print(f"  ✗ Slack error: {result.get('error')}")
        return False


def get_channel_mapping():
    """Fetch Edition_ID → Slack_Channel mapping from awe_editions."""
    client = get_client()
    query = """
    SELECT DISTINCT
        ID AS Edition_ID,
        Title,
        Slack_Channel,
        `*Slack ID - Current Editor` AS Editor_Slack_ID
    FROM `storm-pub-amazon-sales.airtable.awe_editions`
    WHERE Slack_Channel IS NOT NULL AND Slack_Channel != ''
    """
    df = client.query(query).to_dataframe()

    mapping = {}
    for _, row in df.iterrows():
        mapping[row["Edition_ID"]] = {
            "channel": row["Slack_Channel"],
            "title": row["Title"],
            "editor_slack_id": row.get("Editor_Slack_ID") or "",
        }
    return mapping


def extract_edition_id(filename):
    """Extract edition_id from filename like Title_1153.png or Title_1153_GB.png"""
    basename = os.path.basename(filename).replace(".png", "")
    parts = basename.rsplit("_", 2)
    for part in reversed(parts):
        try:
            return int(part)
        except ValueError:
            continue
    return None


from collections import defaultdict

def send_reports(report_type, local_filepaths):
    """
    Send reports to Slack grouped by Edition_ID.
    GB and US reports for the same book will appear under one message.
    """

    if not SLACK_BOT_TOKEN:
        print("SLACK_BOT_TOKEN not set — skipping Slack delivery")
        return 0

    if not local_filepaths:
        print("No files to send")
        return 0

    print(f"\nSending {len(local_filepaths)} {report_type} reports to Slack...\n")

    channel_map = get_channel_mapping()

    sent = 0
    skipped = 0

    # Group files by edition_id
    grouped = defaultdict(list)

    for filepath in local_filepaths:
        edition_id = extract_edition_id(filepath)

        if edition_id is None:
            print(f"  ⚠ Could not extract Edition ID from: {filepath}")
            skipped += 1
            continue

        grouped[edition_id].append(filepath)

    # Send grouped reports
    for edition_id, files in grouped.items():

        info = channel_map.get(edition_id)
        if not info:
            print(f"  ⚠ No Slack channel for Edition {edition_id}")
            skipped += 1
            continue

        channel_name = info["channel"]
        book_title = info["title"]

        channel_id = lookup_channel_id(channel_name)
        if not channel_id:
            skipped += 1
            continue

        messages = {
            "scorecard": f"📊 Daily Facebook Ads Analysis: *{book_title}*<@U053NJZ5DT7> <@U0A861XPXK3>",
            "genre": f"📈 Genre Analysis: *{book_title}* <@U053NJZ5DT7> <@U0A861XPXK3>",
            "weekly": f"📋 Weekly Report: *{book_title}* <@U053NJZ5DT7>  <@U0A861XPXK3>",
        }


        if report_type == "launch":
            milestone_labels = {"30d": "30-Day", "90d": "90-Day", "12m": "12-Month"}
            milestones = []
            for f in files:
                base = os.path.basename(f).replace(".png", "")
                suffix = base.rsplit("_", 1)[-1]
                if suffix in milestone_labels:
                    milestones.append(milestone_labels[suffix])
            milestone_str = " / ".join(milestones) if milestones else "Launch"
            editor_mention = f" {info['editor_slack_id']}" if info.get("editor_slack_id") else ""
            message = f"🚀 {milestone_str} Milestone Report: *{book_title}*{editor_mention}<@U042HE7HJJW> <@U04QUDS0EKS>"
        else:
            message = messages.get(report_type, f"📄 Report: *{book_title}*")
    
        first = True

        for filepath in files:

            title = os.path.basename(filepath)

            success = upload_file_to_slack(
                channel_id,
                filepath,
                title,
                message if first else None
            )

            first = False

            if success:
                print(f"  ✓ Sent {title} → {channel_name}")
            else:
                skipped += 1

            time.sleep(0.5)

        sent += 1

    # Clean up files
    for filepath in local_filepaths:
        try:
            os.remove(filepath)
        except Exception:
            pass

    print(f"\nDone! Sent: {sent}, Skipped: {skipped}")

    return sent


if __name__ == "__main__":
    print("Testing Slack connection...")
    if not SLACK_BOT_TOKEN:
        print("Set SLACK_BOT_TOKEN env var first")
    else:
        resp = requests.get(
            f"{SLACK_BASE_URL}/auth.test",
            headers=_headers(),
            timeout=10,
        )
        data = resp.json()
        if data.get("ok"):
            print(f"Connected as: {data.get('bot_id')} in workspace: {data.get('team')}")
        else:
            print(f"Auth failed: {data.get('error')}")