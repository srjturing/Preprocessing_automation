import os
import time
import json
import requests
from urllib.parse import unquote
# === CONFIGURATION ===
UPLOAD_URL = "https://labeling-m.turing.com/api/batches/upload/rlhf-metadata"
API_BASE = "https://labeling-m.turing.com/api/batches"
TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImpvZ2FuaS5zQHR1cmluZy5jb20iLCJzdWIiOjkzOSwiaWF0IjoxNzYwNDM2Mzg4LCJleHAiOjE3NjEwNDExODh9.X8KamvYOyQ6zn5QY7f9QIRNu02m_IQBPi3lWGkQVLAU"  # rotate the token you pasted publicly
CSV_FOLDER = r"C:\Users\Admin\Desktop\Turing\upload"
PROJECT_ID = 204
PROJECT_NAME = "test-tool"
LOG_FILE = "batch_upload_log_individual.json"
DELAY_BETWEEN_BATCHES = 60  # Seconds
AUTH_HEADERS = {"Authorization": TOKEN}  # use for upload & import
JSON_HEADERS = {"Authorization": TOKEN, "Content-Type": "application/json"}  # use for JSON requests
def upload_csv(file_path):
    with open(file_path, "rb") as fp:
        files = {"file": (os.path.basename(file_path), fp)}
        data = {"project_type": "rlhf"}   # 'pdf' if you're doing a PDF project
        r = requests.post(UPLOAD_URL, headers=AUTH_HEADERS, data=data, files=files)
        r.raise_for_status()              # will raise only on 4xx/5xx
        link = r.json()["fileLink"]       # e.g. https://storage.googleapis.com/rlhf-batch-uploads/<object-name>.csv
        object_name = unquote(link.rsplit("/", 1)[-1])
        return link, object_name
def create_batch_rlhf(object_name, batch_name, file_link):
    payload = {
        "name": batch_name,
        "folder": file_link,          # IMPORTANT: full URL as required by API
        "description": "",
        "status": "draft",
        "files": [],
        "isRLHFFolder": True,         # IMPORTANT: tells backend to read from RLHF GCS
        "project": {
            "id": PROJECT_ID,
            "name": PROJECT_NAME,
            "status": "ongoing",
            "projectType": "rlhf",
            "readonly": False
        },
        "sourceFiles": [object_name],  # keep aligned with folder
        "projectId": PROJECT_ID,
        "projectType": "rlhf"
    }
    r = requests.post(API_BASE, json=payload, headers=JSON_HEADERS)
    r.raise_for_status()
    return r.json()["id"]
def import_batch(batch_id):
    url = f"{API_BASE}/{batch_id}/import-rlhf"
    print(f":link: Import URL: {url}")
    r = requests.post(url, headers=AUTH_HEADERS)
    print(f":bar_chart: Response Status Code: {r.status_code}")
    print(f":clipboard: Response Headers: {dict(r.headers)}")
    print(f":memo: Response Text: {repr(r.text)}")
    r.raise_for_status()
    if r.text.strip():
        return r.json()
    else:
        print(":warning: Empty response body")
        return {}
def main():
    print(":mag: Scanning folder for CSV files...")
    all_files = [f for f in os.listdir(CSV_FOLDER) if f.lower().endswith(".csv")]
    if not all_files:
        print(":x: No CSV files found in the folder.")
        return
    print(f":white_check_mark: Found {len(all_files)} CSV file(s) to upload.")
    log_data = {}
    for csv_file in sorted(all_files):
        prefix = os.path.splitext(csv_file)[0]
        log_entry = {"file": csv_file, "status": "pending", "batch_id": None, "error": None}
        try:
            full_path = os.path.join(CSV_FOLDER, csv_file)
            print(f":rocket: Uploading {csv_file} ...")
            file_link, object_name = upload_csv(full_path)
            print(f":white_check_mark: Uploaded → {file_link}")
            print(f"   Object name → {object_name}")
            print(f":package: Creating batch: {prefix}")
            batch_id = create_batch_rlhf(object_name, batch_name=prefix, file_link=file_link)
            log_entry["batch_id"] = batch_id
            print(f":white_check_mark: Batch created with ID: {batch_id}")
            print(":inbox_tray: Importing batch...")
            import_batch(batch_id)
            log_entry["status"] = "imported"
            print(":white_check_mark: Import successful.")
        except requests.HTTPError as e:
            body = e.response.text if e.response is not None else str(e)
            headers = dict(e.response.headers) if e.response is not None else {}
            status_code = e.response.status_code if e.response is not None else "Unknown"
            log_entry["status"] = "failed"
            log_entry["error"] = f"{e} | status: {status_code} | headers: {headers} | body: {body}"
            print(f":x: HTTP Error processing {csv_file}:")
            print(f"   Status Code: {status_code}")
            print(f"   Headers: {headers}")
            print(f"   Body: {body}")
            print(f"   Exception: {e}")
        except Exception as e:
            log_entry["status"] = "failed"
            log_entry["error"] = str(e)
            print(f":x: Error processing {csv_file}: {e}")
        log_data[prefix] = log_entry
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(log_data, f, indent=2)
        print(f":hourglass_flowing_sand: Waiting {DELAY_BETWEEN_BATCHES} seconds before next upload...")
        time.sleep(DELAY_BETWEEN_BATCHES)
    print(":tada: :white_check_mark: All CSVs processed.")
if __name__ == "__main__":
    main()