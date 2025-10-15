# app.py
# python -m streamlit run app.py

import streamlit as st
import io
import json
import re
import pandas as pd
from typing import List, Dict, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
import zipfile
import httplib2
from google_auth_httplib2 import AuthorizedHttp
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
UPLOAD_URL = "https://labeling-m.turing.com/api/batches/upload/rlhf-metadata"
API_BASE   = "https://labeling-m.turing.com/api/batches"

DEFAULT_CONNECT_TIMEOUT = 20      # seconds to establish TCP/TLS
DEFAULT_READ_TIMEOUT    = 600     # seconds to upload/import large CSVs
DEFAULT_TOTAL_RETRIES   = 4       # number of retry attempts for transient failures

# ─────────────────────────────────────────────────────────────────────────────
# STYLES
# ─────────────────────────────────────────────────────────────────────────────
CSS = """
<style>
.block-container { padding-top: 1.2rem; padding-bottom: 3rem; }
:root { --radius: 14px; }
h1, h2, h3 { letter-spacing: 0.2px; }
.mode-btn { width: 100%; border-radius: 12px; padding: 0.9rem 0.75rem; font-weight: 600;
  border: 1px solid rgba(0,0,0,0.08); background: #f8f9fb; }
.mode-btn:hover { background: #eef1f6; }
.section-title { font-size: 1.05rem; font-weight: 700; margin-bottom: 0.35rem; }
.section-help { color: #5f6c7b; font-size: 0.9rem; margin-bottom: 0.6rem; }
label, .stRadio label, .stSelectbox label, .stNumberInput label { font-weight: 600; }
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# HTTP CLIENT WITH RETRIES / TIMEOUTS
# ─────────────────────────────────────────────────────────────────────────────
def make_session(verify_ssl: bool = True) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=DEFAULT_TOTAL_RETRIES,
        read=DEFAULT_TOTAL_RETRIES,
        connect=DEFAULT_TOTAL_RETRIES,
        status=DEFAULT_TOTAL_RETRIES,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "POST", "DELETE", "OPTIONS", "TRACE", "PATCH"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.verify = verify_ssl
    s.headers.update({"User-Agent": "turing-preprocess-uploader/1.0"})
    return s

def timeout_tuple(connect: int, read: int):
    return (connect, read)

def _auth_headers(token: str) -> dict:
    token = (token or "").strip()
    if token and not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    return {"Authorization": token} if token else {}

def _json_headers(token: str) -> dict:
    h = _auth_headers(token)
    h["Content-Type"] = "application/json"
    return h

def _format_http_error(resp: requests.Response) -> str:
    try:
        body = resp.text
    except Exception:
        body = "<no body>"
    return (f"HTTP {resp.status_code}\n"
            f"URL: {resp.request.method} {resp.url}\n"
            f"Headers: {dict(resp.headers)}\n"
            f"Body: {body[:2000]}")

# ─────────────────────────────────────────────────────────────────────────────
# TOOL UPLOAD API (robust)
# ─────────────────────────────────────────────────────────────────────────────
def upload_csv_bytes(session: requests.Session, csv_bytes: bytes, filename: str, token: str,
                     connect_timeout: int, read_timeout: int) -> Tuple[str, str]:
    """
    Step 1: Upload CSV to RLHF GCS via your upload endpoint.
    Returns (file_link, object_name).
    Raises Exception with detailed diagnostics on failure.
    """
    headers = _auth_headers(token)
    files = {"file": (filename, csv_bytes, "text/csv")}
    data  = {"project_type": "rlhf"}  # change to "pdf" for PDF projects
    t = timeout_tuple(connect_timeout, read_timeout)

    resp = session.post(UPLOAD_URL, headers=headers, data=data, files=files, timeout=t)
    if not resp.ok:
        raise Exception("Upload step failed:\n" + _format_http_error(resp))

    try:
        payload = resp.json()
    except Exception:
        raise Exception("Upload step returned non-JSON:\n" + _format_http_error(resp))

    link = payload.get("fileLink")
    if not link:
        raise Exception(f"Upload step missing 'fileLink' in response:\n{payload}")

    from urllib.parse import unquote
    object_name = unquote(link.rsplit("/", 1)[-1])
    return link, object_name

def create_batch_rlhf(session: requests.Session, object_name: str, batch_name: str, file_link: str, *,
                      token: str, project_id: int, project_name: str,
                      connect_timeout: int, read_timeout: int) -> int:
    """
    Step 2: Create batch pointing to the uploaded CSV.
    """
    payload = {
        "name": batch_name,
        "folder": file_link,
        "description": "",
        "status": "draft",
        "files": [],
        "isRLHFFolder": True,
        "project": {
            "id": int(project_id),
            "name": project_name,
            "status": "ongoing",
            "projectType": "rlhf",
            "readonly": False
        },
        "sourceFiles": [object_name],
        "projectId": int(project_id),
        "projectType": "rlhf"
    }
    t = timeout_tuple(connect_timeout, read_timeout)
    resp = session.post(API_BASE, json=payload, headers=_json_headers(token), timeout=t)
    if not resp.ok:
        raise Exception("Create-batch step failed:\n" + _format_http_error(resp))
    try:
        return resp.json()["id"]
    except Exception:
        raise Exception("Create-batch returned invalid JSON:\n" + _format_http_error(resp))

def import_batch(session: requests.Session, batch_id: int, *, token: str,
                 connect_timeout: int, read_timeout: int) -> dict:
    """
    Step 3: Trigger import for the created batch.
    """
    url = f"{API_BASE}/{batch_id}/import-rlhf"
    t = timeout_tuple(connect_timeout, read_timeout)
    resp = session.post(url, headers=_auth_headers(token), timeout=t)
    if not resp.ok:
        raise Exception("Import step failed:\n" + _format_http_error(resp))
    return resp.json() if resp.text.strip() else {}

def upload_pipeline(csv_bytes: bytes, filename: str, *, token: str, project_id: int, project_name: str,
                    session: requests.Session, connect_timeout: int, read_timeout: int) -> Tuple[bool, str]:
    """
    Full flow for one CSV: upload → create batch → import.
    Returns (ok, message).
    """
    try:
        with st.status(f"Uploading {filename}", expanded=True) as stat:
            st.write("Step 1/3: Uploading CSV bytes …")
            file_link, object_name = upload_csv_bytes(session, csv_bytes, filename, token, connect_timeout, read_timeout)

            st.write(f"Step 2/3: Creating batch …")
            batch_name = filename.rsplit(".", 1)[0]
            batch_id = create_batch_rlhf(session, object_name, batch_name, file_link,
                                         token=token, project_id=int(project_id), project_name=project_name,
                                         connect_timeout=connect_timeout, read_timeout=read_timeout)

            st.write("Step 3/3: Importing batch …")
            import_batch(session, batch_id, token=token, connect_timeout=connect_timeout, read_timeout=read_timeout)

            stat.update(label=f"Done: {filename}", state="complete")
        return True, f"✅ Uploaded and imported. Batch ID: {batch_id}"
    except Exception as e:
        return False, f"❌ {e}"

# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE DRIVE HELPERS
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def authenticate_drive():
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        SERVICE_ACCOUNT_FILE = "turing-genai-ws-58339643dd3f.json"
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=300))
    return build("drive", "v3", http=http, cache_discovery=False)

def _pick_levels_example(drive_df: pd.DataFrame):
    if drive_df.empty or "full_path" not in drive_df.columns:
        return []
    lens = drive_df["full_path"].apply(lambda p: len(p) if isinstance(p, list) else 0)
    idx = lens.idxmax()
    example = drive_df.at[idx, "full_path"]
    return example if isinstance(example, list) else []

def show_levels_example_dialog(levels_list):
    if not levels_list:
        return
    lines = [f"Level_{i}: {part}" for i, part in enumerate(levels_list)]
    note = "Note: The last level corresponds to the image file name."
    try:
        with st.dialog("Example: understanding `level_*` columns"):
            st.write("Here’s how folder path parts map to the `level_*` columns:")
            st.code("\n".join(lines))
            st.caption(note)
            if st.button("Got it", type="primary", key="levels_got_it"):
                st.session_state.show_levels_modal = False
                st.rerun()
    except Exception:
        st.info("Example: understanding `level_*` columns")
        st.code("\n".join(lines))
        st.caption(note)
        if st.button("Dismiss", key="levels_dismiss"):
            st.session_state.show_levels_modal = False
            st.rerun()

def show_drive_access_dialog():
    msg = ("If you're sharing a Google Drive link, please ensure access is granted to:")
    def _content():
        st.write(msg)
        st.code("char-automations@turing-gpt.iam.gserviceaccount.com")
        st.code("labeling-tool-dev@turing-gpt.iam.gserviceaccount.com")
        st.code("google-drive-access@turing-genai-ws.iam.gserviceaccount.com")
        if st.button("Got it", type="primary", key="drive_access_got_it"):
            st.session_state.show_access_modal = False
            try: st.rerun()
            except Exception:
                try: st.experimental_rerun()
                except Exception: pass
    try:
        @st.dialog("Google Drive sharing reminder")
        def _dlg(): _content()
        _dlg()
    except Exception:
        st.info(msg)
        st.code("char-automations@turing-gpt.iam.gserviceaccount.com")
        st.code("labeling-tool-dev@turing-gpt.iam.gserviceaccount.com")
        st.code("google-drive-access@turing-genai-ws.iam.gserviceaccount.com")
        if st.button("Dismiss", key="drive_access_dismiss"):
            st.session_state.show_access_modal = False
            try: st.rerun()
            except Exception:
                try: st.experimental_rerun()
                except Exception: pass

def list_drive_images_recursive(service, parent_id: str, current_path=None, results=None) -> List[Dict]:
    if current_path is None: current_path = []
    if results is None: results = []
    query = (f"'{parent_id}' in parents and trashed = false and "
             "(mimeType = 'application/vnd.google-apps.folder' or mimeType contains 'image/')")
    page_token = None
    max_retries, backoff = 5, 1.5
    while True:
        req = (service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token, pageSize=1000,
            corpora="allDrives", supportsAllDrives=True, includeItemsFromAllDrives=True))
        attempt = 0
        while True:
            try:
                resp = req.execute(num_retries=3)
                break
            except Exception:
                attempt += 1
                if attempt >= max_retries: raise
                time.sleep(backoff ** attempt)
        for f in resp.get("files", []):
            mime = f.get("mimeType", "")
            if mime == "application/vnd.google-apps.folder":
                list_drive_images_recursive(service, f["id"], current_path + [f["name"]], results)
            elif mime.startswith("image/"):
                results.append({
                    "image_name": f["name"],
                    "image_link": f"https://drive.google.com/file/d/{f['id']}/view",
                    "full_path": current_path + [f["name"]],
                })
        page_token = resp.get("nextPageToken")
        if not page_token: break
    return results

def build_drive_dataframe(folder_id: str) -> Tuple[pd.DataFrame, List[str]]:
    service = authenticate_drive()
    image_data = list_drive_images_recursive(service, folder_id)
    if not image_data: return pd.DataFrame(), []
    drive_df = pd.DataFrame(image_data)
    max_depth = max(len(p) for p in drive_df["full_path"])
    for i in range(max_depth):
        drive_df[f"level_{i}"] = drive_df["full_path"].apply(lambda x: x[i] if i < len(x) else "")
    drive_df["path_preview"] = drive_df["full_path"].apply(lambda x: "/".join(x))
    level_cols = [f"level_{i}" for i in range(max_depth)]
    available_fields = ["image_name"] + level_cols + ["image_link", "path_preview"]
    return drive_df, available_fields

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
def load_tabular(uploaded_file) -> Tuple[pd.DataFrame, List[str]]:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
        return df, list(df.columns)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        xls = pd.ExcelFile(uploaded_file)
        sheet = st.selectbox("Select sheet", options=xls.sheet_names, index=0)
        df = xls.parse(sheet)
        return df, list(df.columns)
    raise ValueError("Unsupported file type. Please upload .csv, .xlsx, or .xls")

def get_value(row: pd.Series, field: str) -> str:
    if field == ".": return "."
    return str(row.get(field, "."))

def reset_counts():
    st.session_state.counts_confirmed = False
    st.session_state.n_cols = None
    st.session_state.meta_count = None

# ─────────────────────────────────────────────────────────────────────────────
# JSON HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _flatten_leaves(d: dict, parent_prefix: str) -> dict:
    out = {}
    for k, v in d.items():
        key_path = f"{parent_prefix}-{k}" if parent_prefix else k
        if isinstance(v, dict):
            out.update(_flatten_leaves(v, key_path))
        else:
            out[key_path] = v
    return out

def build_json_dataframe(uploaded_json_file) -> Tuple[pd.DataFrame, List[str]]:
    text_fp = io.StringIO(uploaded_json_file.getvalue().decode("utf-8"))
    data = json.load(text_fp)
    file_meta = data.get("fileMetadata", {})
    file_meta_flat = {f"fileMetadata-{k}": v for k, v in file_meta.items()}
    rows = []
    for wi in data.get("workitems", []):
        row = {}
        if "workItemId" in wi:
            row["workitems-workItemId"] = wi["workItemId"]
        for k, v in wi.items():
            if k == "workItemId": continue
            if isinstance(v, dict):
                if k == "inputData":
                    images = []
                    for ik, iv in v.items():
                        if re.fullmatch(r"Image_\\d+", ik):
                            images.append(iv)
                        elif isinstance(iv, dict):
                            nested = _flatten_leaves(iv, f"workitems-{k}")
                            row.update(nested)
                        else:
                            row[f"workitems-{k}-{ik}"] = iv
                    if images:
                        row["workitems-inputData-Image"] = ", ".join(images)
                else:
                    row.update(_flatten_leaves(v, f"workitems-{k}"))
            else:
                row[f"workitems-{k}"] = v
        full_row = {**file_meta_flat, **row}
        rows.append(full_row)
    df = pd.DataFrame(rows)
    available_fields = list(df.columns)
    return df, available_fields

# ─────────────────────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Preprocessing Automation", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)
st.title("📦 Preprocessing Automation")

# Advanced uploader options (left sidebar)
with st.sidebar:
    st.markdown("### ⚙️ Network Settings")
    verify_ssl = st.toggle("Verify SSL certificates", value=True,
                           help="Turn OFF only for debugging corporate proxy/TLS issues.")
    connect_timeout = st.number_input("Connect timeout (sec)", min_value=5, max_value=120, value=DEFAULT_CONNECT_TIMEOUT, step=1)
    read_timeout    = st.number_input("Read timeout (sec)", min_value=60, max_value=1800, value=DEFAULT_READ_TIMEOUT, step=30)
    st.caption("If uploads hang/fail, try increasing timeouts. Keep SSL verification ON unless debugging.")
    # Prepare session
    if "http_session" not in st.session_state or st.session_state.get("_verify_ssl") != verify_ssl:
        st.session_state["http_session"] = make_session(verify_ssl=verify_ssl)
        st.session_state["_verify_ssl"] = verify_ssl
session = st.session_state["http_session"]

st.markdown(
    """
<div class="card">
  <div class="section-title">Choose your input mode</div>
  <div class="section-help">Start by selecting how you'd like to provide data.</div>
</div>
""", unsafe_allow_html=True)
st.divider()

# Mode session state
if "mode" not in st.session_state:
    st.session_state.mode = None
    reset_counts()
if "show_access_modal" not in st.session_state:
    st.session_state.show_access_modal = False
if "show_levels_modal" not in st.session_state:
    st.session_state.show_levels_modal = False
if "levels_example" not in st.session_state:
    st.session_state.levels_example = []
if "levels_modal_seen_for" not in st.session_state:
    st.session_state.levels_modal_seen_for = set()
if "last_folder_id_shown" not in st.session_state:
    st.session_state.last_folder_id_shown = None

col1, col2, col3, col4 = st.columns(4)
with col1:
    if st.button("📄 CSV / Excel", use_container_width=True):
        st.session_state.mode = "CSV/Excel"; reset_counts(); st.session_state.show_access_modal = False
with col2:
    if st.button("🗂️ Drive Folder ID", use_container_width=True):
        st.session_state.mode = "Drive Folder ID"; reset_counts(); st.session_state.show_access_modal = True
with col3:
    if st.button("🔗 Both (CSV + Drive)", use_container_width=True):
        st.session_state.mode = "Both (CSV + Drive)"; reset_counts(); st.session_state.show_access_modal = True
with col4:
    if st.button("🧾 JSON file", use_container_width=True):
        st.session_state.mode = "JSON"; reset_counts(); st.session_state.show_access_modal = False

mode = st.session_state.mode
if not mode: st.stop()

if st.session_state.show_access_modal and mode in ("Drive Folder ID", "Both (CSV + Drive)"):
    show_drive_access_dialog()

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# MODES
# ─────────────────────────────────────────────────────────────────────────────
if mode == "CSV/Excel":
    st.markdown('<div class="section-title">Upload CSV / Excel</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        uploaded_file = st.file_uploader("Upload .csv, .xlsx, or .xls", type=["csv", "xlsx", "xls"])
        st.markdown('</div>', unsafe_allow_html=True)
    if not uploaded_file: st.stop()

    try:
        base_df, csv_cols = load_tabular(uploaded_file)
        available_fields = csv_cols[:]
        st.success(f"Loaded {len(base_df)} rows from the uploaded file.")
        st.dataframe(base_df.head(20), use_container_width=True)
        _csv_buf = io.StringIO(); base_df.to_csv(_csv_buf, index=False)
        st.download_button("⬇️ Download full table (CSV)", data=_csv_buf.getvalue(),
                           file_name="input_preview.csv", mime="text/csv",
                           use_container_width=True, key="dl_csv_preview")
        source_type = "csv"
    except Exception as e:
        st.error(f"Failed to read the uploaded file: {e}"); st.stop()

elif mode == "Drive Folder ID":
    st.markdown('<div class="section-title">Enter Google Drive Folder ID</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        folder_id = st.text_input("Google Drive Folder ID (images)")
        st.markdown('</div>', unsafe_allow_html=True)
    if not folder_id: st.stop()

    try:
        with st.spinner("Scanning Google Drive folder recursively..."):
            drive_df, drive_fields = build_drive_dataframe(folder_id)
        if drive_df.empty: st.warning("No images found."); st.stop()

        if st.session_state.last_folder_id_shown != folder_id:
            st.session_state.levels_example = _pick_levels_example(drive_df)
            st.session_state.show_levels_modal = True
            st.session_state.last_folder_id_shown = folder_id
        if folder_id not in st.session_state.levels_modal_seen_for:
            st.session_state.levels_example = _pick_levels_example(drive_df)
            st.session_state.show_levels_modal = True
            st.session_state.levels_modal_seen_for.add(folder_id)
        if st.session_state.show_levels_modal:
            show_levels_example_dialog(st.session_state.levels_example)

        base_df = drive_df
        available_fields = drive_fields[:]
        st.success(f"Discovered {len(base_df)} images.")
        _display_cols = ["image_name", "path_preview", "image_link"]
        st.dataframe(base_df[_display_cols].head(20), use_container_width=True)
        _drive_buf = io.StringIO(); base_df[_display_cols].to_csv(_drive_buf, index=False)
        st.download_button("⬇️ Download full table (CSV)", data=_drive_buf.getvalue(),
                           file_name="drive_preview.csv", mime="text/csv",
                           use_container_width=True, key="dl_drive_preview")
        source_type = "drive"
    except Exception as e:
        st.error(f"Drive error: {e}"); st.stop()

elif mode == "Both (CSV + Drive)":
    st.markdown('<div class="section-title">Provide both sources</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        left, right = st.columns(2)
        with left:
            uploaded_file = st.file_uploader("Upload .csv, .xlsx, or .xls", type=["csv", "xlsx", "xls"])
        with right:
            folder_id = st.text_input("Google Drive Folder ID (images)")
        st.markdown('</div>', unsafe_allow_html=True)
    if not uploaded_file or not folder_id:
        st.info("Provide both a CSV/Excel file and a Drive Folder ID."); st.stop()

    try:
        csv_df, csv_cols = load_tabular(uploaded_file)
    except Exception as e:
        st.error(f"Failed to read the uploaded file: {e}"); st.stop()
    try:
        with st.spinner("Scanning Google Drive folder recursively..."):
            drive_df, drive_fields = build_drive_dataframe(folder_id)
        if drive_df.empty: st.warning("No images found."); st.stop()
    except Exception as e:
        st.error(f"Drive error: {e}"); st.stop()

    st.success(f"CSV rows: {len(csv_df)} • Drive images: {len(drive_df)}")
    with st.expander("📄 CSV Columns"):
        st.write(csv_cols); st.dataframe(csv_df.head(10), use_container_width=True)
    with st.expander("🗂️ Drive Fields"):
        st.write(drive_fields); st.dataframe(drive_df[["image_name", "path_preview", "image_link"]].head(10), use_container_width=True)

    if st.session_state.last_folder_id_shown != folder_id:
        st.session_state.levels_example = _pick_levels_example(drive_df)
        st.session_state.show_levels_modal = True
        st.session_state.last_folder_id_shown = folder_id
    if folder_id not in st.session_state.levels_modal_seen_for:
        st.session_state.levels_example = _pick_levels_example(drive_df)
    st.session_state.show_levels_modal = True
    st.session_state.levels_modal_seen_for.add(folder_id)
    if st.session_state.show_levels_modal:
        show_levels_example_dialog(st.session_state.levels_example)

    st.markdown("### 🔗 Choose Mapping Keys")
    st.caption("Pick one column from CSV and one field from Drive. These act as unique keys to merge.")
    map_col_csv = st.selectbox("CSV key column", options=csv_cols, index=0)
    map_col_drive = st.selectbox("Drive key field", options=drive_fields, index=0)
    norm_col = st.checkbox("Normalize keys (strip & lowercase) before merging", value=True)

    def _normalize_series(s: pd.Series) -> pd.Series:
        s = s.astype(str)
        return s.str.strip().str.lower() if norm_col else s

    csv_key = _normalize_series(csv_df[map_col_csv])
    drive_key = _normalize_series(drive_df[map_col_drive])

    dup_csv = csv_key.duplicated(keep=False).sum()
    dup_drive = drive_key.duplicated(keep=False).sum()
    if dup_csv > 0: st.warning(f"CSV mapping key has {dup_csv} duplicate entries.")
    if dup_drive > 0: st.warning(f"Drive mapping key has {dup_drive} duplicate entries.")

    csv_prefixed = csv_df.copy(); csv_prefixed.columns = [f"csv:{c}" for c in csv_prefixed.columns]
    drive_prefixed = drive_df.copy(); drive_prefixed.columns = [f"drive:{c}" for c in drive_prefixed.columns]
    csv_prefixed["_join_key"] = csv_key.values; drive_prefixed["_join_key"] = drive_key.values
    join_how = st.selectbox("Merge strategy", options=["inner", "left", "right", "outer"], index=0)
    merged = pd.merge(csv_prefixed, drive_prefixed, on="_join_key", how=join_how)
    if merged.empty: st.error("Merged result is empty."); st.stop()

    st.success(f"Merged rows: {len(merged)} (join={join_how})")
    with st.expander("Preview merged data"):
        st.dataframe(merged.head(20), use_container_width=True)

    _merge_buf = io.StringIO(); merged.to_csv(_merge_buf, index=False)
    st.download_button("⬇️ Download full merged table (CSV)", data=_merge_buf.getvalue(),
                       file_name="merged_preview.csv", mime="text/csv",
                       use_container_width=True, key="dl_merged_preview")

    base_df = merged.drop(columns=["_join_key"])
    available_fields = list(base_df.columns)
    source_type = "both"

elif mode == "JSON":
    st.markdown('<div class="section-title">Upload JSON</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        uploaded_json = st.file_uploader("Upload .json", type=["json"])
        st.markdown('</div>', unsafe_allow_html=True)
    if not uploaded_json: st.stop()

    try:
        with st.spinner("Parsing JSON and extracting deepest-leaf fields..."):
            df_json, json_fields = build_json_dataframe(uploaded_json)
        if df_json.empty: st.warning("JSON contained no workitems."); st.stop()

        base_df = df_json
        available_fields = json_fields[:]
        source_type = "json"

        st.success(f"Parsed {len(base_df)} workitems from JSON.")
        st.dataframe(base_df.head(20), use_container_width=True)
        _base_buf = io.StringIO(); base_df.to_csv(_base_buf, index=False)
        st.download_button("⬇️ Download full table (CSV)", data=_base_buf.getvalue(),
                           file_name="input_preview.json.csv", mime="text/csv",
                           use_container_width=True, key="dl_json_preview")
        with st.expander("Available fields from JSON"): st.write(available_fields)
    except Exception as e:
        st.error(f"JSON parse error: {e}"); st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# METADATA TOGGLE
# ─────────────────────────────────────────────────────────────────────────────
if "include_metadata" not in st.session_state:
    st.session_state.include_metadata = True
try:
    st.session_state.include_metadata = st.toggle(
        "Metadata needed?", value=st.session_state.include_metadata,
        help="Turn OFF if you don't want a 'metadata' JSON column in the output.")
except Exception:
    st.session_state.include_metadata = st.checkbox(
        "Metadata needed?", value=st.session_state.include_metadata,
        help="Turn OFF if you don't want a 'metadata' JSON column in the output.")

# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT CONFIG: STEP 1 — ENTER COUNTS
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.markdown("### 🧩 Configure Output Columns")

if "counts_confirmed" not in st.session_state:
    st.session_state.counts_confirmed = False
if "n_cols" not in st.session_state:
    st.session_state.n_cols = None
if "meta_count" not in st.session_state:
    st.session_state.meta_count = None

with st.form("config_counts_form", clear_on_submit=False):
    help_text = "First, enter how many output columns you want"
    if st.session_state.include_metadata:
        help_text += " and how many metadata key–value pairs to include."
    st.markdown(f'<div class="section-help">{help_text}</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        label = "Number of output columns"
        label += " (1st is metadata)" if st.session_state.include_metadata else " (no metadata column)"
        n_cols_input = st.number_input(label, min_value=1, max_value=50, value=4, step=1, format="%d")
    with c2:
        if st.session_state.include_metadata:
            meta_count_input = st.number_input("Number of metadata fields", min_value=0, max_value=50, value=2, step=1, format="%d")
        else:
            st.empty(); meta_count_input = 0
    counts_submitted = st.form_submit_button("Continue")

if counts_submitted:
    st.session_state.n_cols = int(n_cols_input)
    st.session_state.meta_count = int(meta_count_input) if st.session_state.include_metadata else 0
    st.session_state.counts_confirmed = True
    st.toast("Counts confirmed. Configure fields below.", icon="✅")

if not st.session_state.counts_confirmed: st.stop()

n_cols = st.session_state.n_cols
meta_count = st.session_state.meta_count

# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT CONFIG: STEP 2 — FIELD SELECTIONS
# ─────────────────────────────────────────────────────────────────────────────
output_colnames_fixed = [
    "metadata","workitem_id","image","image_name","image_link","prompt","question",
    "model_a","model_b","file_path","video_name",
]

with st.form("output_config_form"):
    st.markdown('<div class="section-help">Now choose the metadata pairs (if enabled) and map each output column to a source field.</div>', unsafe_allow_html=True)

    metadata_pairs = []
    if st.session_state.include_metadata and meta_count > 0:
        st.markdown("**Metadata (JSON) fields**")
        for i in range(meta_count):
            c1, c2 = st.columns([1, 1])
            with c1: key_name = st.text_input(f"Metadata key {i+1}", key=f"meta_key_{i}")
            with c2:
                src_options = ["."] + available_fields
                value_field = st.selectbox(f"Value for key {i+1}", options=src_options, key=f"meta_val_{i}")
            if key_name: metadata_pairs.append((key_name, value_field))

    chosen_output_cols, chosen_sources = [], {}
    start_idx = 1
    if st.session_state.include_metadata:
        chosen_output_cols = ["metadata"]; chosen_sources["metadata"] = None; start_idx = 2
    if n_cols >= start_idx:
        st.markdown("**Output columns**")
        name_options = [x for x in output_colnames_fixed if x != "metadata"]
        for j in range(start_idx, n_cols + 1):
            c1, c2 = st.columns([1, 1])
            with c1:
                colname = st.selectbox(f"Name for output column #{j}", options=name_options, key=f"out_col_{j}")
            with c2:
                src = st.selectbox(f"Data for '{colname}'", options=["."] + available_fields, key=f"out_src_{j}")
            chosen_output_cols.append(colname); chosen_sources[colname] = src
    submitted = st.form_submit_button("Build Output CSV")

if submitted:
    out_df = pd.DataFrame()
    if st.session_state.include_metadata:
        def build_metadata(row: pd.Series) -> str:
            if meta_count == 0 or len(metadata_pairs) == 0: return "{}"
            md = {k: get_value(row, field) for k, field in metadata_pairs}
            try: return json.dumps(md, ensure_ascii=False)
            except Exception: return json.dumps({k: str(v) for k, v in md.items()}, ensure_ascii=False)
        out_df["metadata"] = base_df.apply(build_metadata, axis=1)

    for name in chosen_output_cols:
        if name == "metadata": continue
        src_field = chosen_sources.get(name)
        out_df[name] = "." if src_field is None else base_df.apply(lambda r: get_value(r, src_field), axis=1)

    st.success(f"✅ Generated {len(out_df)} rows and {out_df.shape[1]} columns.")
    st.dataframe(out_df.head(20), use_container_width=True)

    st.session_state["out_df"] = out_df
    st.session_state["available_fields_for_split"] = available_fields
    st.session_state["base_df_for_split"] = base_df
    if "split_field" not in st.session_state: st.session_state["split_field"] = "None"

# ─────────────────────────────────────────────────────────────────────────────
# SPLIT, DOWNLOAD & UPLOAD
# ─────────────────────────────────────────────────────────────────────────────
if "out_df" in st.session_state:
    st.markdown("#### ➗ Split, Download & Upload")
    out_df = st.session_state["out_df"]
    available_fields = st.session_state["available_fields_for_split"]
    base_df = st.session_state["base_df_for_split"]

    def _safe_name(s: str) -> str:
        s = str(s) if s is not None else "UNSPECIFIED"
        s = s.strip() or "UNSPECIFIED"
        s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
        return s[:80]

    split_options = ["None"] + sorted(list(dict.fromkeys(map(str, available_fields))))
    default_split = st.session_state.get("split_field", "None")
    if default_split not in split_options: default_split = "None"

    split_field = st.selectbox(
        "Split by (choose a field from your input). If 'None', a single CSV is produced.",
        options=split_options, index=split_options.index(default_split), key="split_field")

    # SINGLE CSV PATH
    is_none = str(split_field).strip().lower() == "none"
    if is_none:
        # Build & cache
        single_csv_bytes_key = "__single_csv_bytes__"
        single_csv_sig_key = "__single_csv_sig__"
        sig = (len(out_df), tuple(out_df.columns))
        if st.session_state.get(single_csv_sig_key) != sig:
            buf = io.StringIO(); out_df.to_csv(buf, index=False)
            st.session_state[single_csv_bytes_key] = buf.getvalue()
            st.session_state[single_csv_sig_key] = sig
        c_dl, c_up = st.columns([0.5, 0.5])
        with c_dl:
            st.download_button("⬇️ Download CSV", data=st.session_state[single_csv_bytes_key],
                               file_name="output.csv", mime="text/csv",
                               key="single_csv_dl", use_container_width=True)
        with c_up:
            want_upload = st.checkbox("Do you want to upload to the tool now?", key="single_want_upload")
            if want_upload:
                token       = st.text_input("TOKEN", type="password", key="single_tok")
                project_id  = st.text_input("PROJECT_ID", key="single_pid")
                project_name= st.text_input("PROJECT_NAME", key="single_pname")
                if st.button("🚀 Upload output.csv", use_container_width=True, key="single_go"):
                    if not (token and project_id and project_name):
                        st.error("Please fill TOKEN, PROJECT_ID and PROJECT_NAME.")
                    else:
                        ok, msg = upload_pipeline(
                            csv_bytes=st.session_state[single_csv_bytes_key].encode("utf-8"),
                            filename="output.csv", token=token,
                            project_id=int(project_id), project_name=project_name,
                            session=session, connect_timeout=connect_timeout, read_timeout=read_timeout)
                        (st.success if ok else st.error)(msg)
        st.stop()  # do not render split UI

    # SPLIT PATH
    series = base_df.get(split_field)
    if series is None:
        st.error("Selected split field not found in the input data."); st.stop()

    groups = {}
    for idx, val in series.items():
        key_val = _safe_name(val if pd.notna(val) else "UNSPECIFIED")
        groups.setdefault(key_val, []).append(idx)

    sel_state_key      = f"__selected_groups__{split_field}"
    merged_csv_key     = f"__merged_selected_csv__{split_field}"
    show_merged_dl_key = f"__show_merged_dl__{split_field}"
    st.session_state.setdefault(sel_state_key, {})
    st.session_state.setdefault(merged_csv_key, None)
    st.session_state.setdefault(show_merged_dl_key, False)

    st.markdown("**Groups**")
    selected_count = 0
    for gval, idxs in sorted(groups.items(), key=lambda x: x[0].lower()):
        c0, c1, c2, c3 = st.columns([0.06, 0.54, 0.20, 0.20])
        with c0:
            checked = st.checkbox("", value=st.session_state[sel_state_key].get(gval, False),
                                  key=f"sel_{split_field}_{gval}")
            st.session_state[sel_state_key][gval] = bool(checked)
            if checked: selected_count += 1
        with c1:
            st.write(f"**{gval}** — {len(idxs)} rows")
        subset_csv_str = out_df.loc[idxs].to_csv(index=False)
        filename = f"output_{_safe_name(split_field)}_{gval}.csv"
        with c2:
            st.download_button("⬇️ Download CSV", data=subset_csv_str,
                               file_name=filename, mime="text/csv",
                               key=f"dl_{split_field}_{gval}", use_container_width=True)
        with c3:
            if st.button("🚀 Upload", key=f"upload_{split_field}_{gval}", use_container_width=True):
                st.session_state[f"_pending_upload_{split_field}_{gval}"] = subset_csv_str

    st.markdown("---")
    st.write(f"Selected groups: **{selected_count} / {len(groups)}**")

    st.markdown("### 📤 Upload to Tool")
    want_upload_split = st.checkbox("Do you want to upload now?", key=f"want_upload_{split_field}")
    if want_upload_split:
        cA, cB, cC, cD = st.columns([0.25, 0.25, 0.25, 0.25])
        with cA: token_bulk        = st.text_input("TOKEN", type="password", key=f"bulk_tok_{split_field}")
        with cB: project_id_bulk   = st.text_input("PROJECT_ID", key=f"bulk_pid_{split_field}")
        with cC: project_name_bulk = st.text_input("PROJECT_NAME", key=f"bulk_pname_{split_field}")
        with cD: delay_secs        = st.number_input("Delay (sec) between uploads", min_value=0, max_value=300, value=0, step=1, key=f"delay_{split_field}")

        c1, c2, c3 = st.columns([0.34, 0.33, 0.33])
        # Download ALL as ZIP
        with c1:
            zip_bytes = io.BytesIO()
            with zipfile.ZipFile(zip_bytes, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for gval, idxs in groups.items():
                    subset_csv = out_df.loc[idxs].to_csv(index=False)
                    zf.writestr(f"output_{_safe_name(split_field)}_{gval}.csv", subset_csv)
            zip_bytes.seek(0)
            st.download_button(f"📦 Download ALL as ZIP ({len(groups)} files)",
                               data=zip_bytes.getvalue(),
                               file_name=f"output_by_{_safe_name(split_field)}.zip",
                               mime="application/zip",
                               key=f"zip_all_dl_{split_field}", use_container_width=True)
        # Apply selection (build merged CSV)
        with c2:
            if st.button("✅ Apply selection"):
                chosen = [g for g, v in st.session_state[sel_state_key].items() if v and g in groups]
                frames = [out_df.loc[groups[g]] for g in chosen]
                merged_df = pd.concat(frames, axis=0, ignore_index=True) if frames else out_df.iloc[0:0]
                st.session_state[merged_csv_key] = merged_df.to_csv(index=False)
                st.session_state[show_merged_dl_key] = True
                st.toast("Selection applied. Merged CSV is ready.", icon="✅")
        # Bulk upload
        with c3:
            st.markdown("**Bulk Upload**")
            mode_choice = st.radio("Which files?", options=["Selected", "All"], horizontal=True, key=f"bulk_mode_{split_field}")
            if st.button("🚀 Upload", key=f"bulk_upload_{split_field}", use_container_width=True):
                if not (token_bulk and project_id_bulk and project_name_bulk):
                    st.error("Please fill TOKEN, PROJECT_ID and PROJECT_NAME.")
                else:
                    targets = [g for g, v in st.session_state[sel_state_key].items() if v and g in groups] if mode_choice == "Selected" else list(groups.keys())
                    if not targets:
                        st.warning("No groups selected.")
                    else:
                        successes, failures = 0, 0
                        for gval in targets:
                            csv_str = out_df.loc[groups[gval]].to_csv(index=False)
                            filename = f"output_{_safe_name(split_field)}_{gval}.csv"
                            ok, msg = upload_pipeline(
                                csv_bytes=csv_str.encode("utf-8"), filename=filename,
                                token=token_bulk, project_id=int(project_id_bulk), project_name=project_name_bulk,
                                session=session, connect_timeout=connect_timeout, read_timeout=read_timeout)
                            if ok: successes += 1
                            else:
                                failures += 1; st.error(f"{filename}: {msg}")
                            if delay_secs: time.sleep(delay_secs)
                        if successes: st.success(f"Uploaded {successes} file(s).")
                        if failures == 0: st.toast("Bulk upload finished.", icon="✅")

        # Handle per-file Upload button clicks (use same creds)
        for gval, idxs in sorted(groups.items(), key=lambda x: x[0].lower()):
            pending_key = f"_pending_upload_{split_field}_{gval}"
            if pending_key in st.session_state and st.session_state[pending_key] is not None:
                if not (token_bulk and project_id_bulk and project_name_bulk):
                    st.error(f"Fill TOKEN, PROJECT_ID and PROJECT_NAME to upload {gval}.")
                else:
                    csv_str = st.session_state[pending_key]
                    filename = f"output_{_safe_name(split_field)}_{gval}.csv"
                    ok, msg = upload_pipeline(
                        csv_bytes=csv_str.encode("utf-8"), filename=filename,
                        token=token_bulk, project_id=int(project_id_bulk), project_name=project_name_bulk,
                        session=session, connect_timeout=connect_timeout, read_timeout=read_timeout)
                    (st.success if ok else st.error)(f"{filename}: {msg}")
                st.session_state[pending_key] = None

    # Merged CSV download + upload
    if st.session_state.get(show_merged_dl_key) and st.session_state.get(merged_csv_key):
        st.download_button("⬇️ Download Selected (Merged CSV)",
                           data=st.session_state[merged_csv_key],
                           file_name=f"merged_selected_by_{_safe_name(split_field)}.csv",
                           mime="text/csv",
                           key=f"merged_selected_dl_{split_field}",
                           use_container_width=True)
        if want_upload_split:
            with st.expander("Upload Selected (Merged CSV)"):
                if not (st.session_state.get(f"bulk_tok_{split_field}") and st.session_state.get(f"bulk_pid_{split_field}") and st.session_state.get(f"bulk_pname_{split_field}")):
                    st.info("Use the credentials above in 📤 Upload to Tool.")
                else:
                    token_m        = st.session_state[f"bulk_tok_{split_field}"]
                    project_id_m   = st.session_state[f"bulk_pid_{split_field}"]
                    project_name_m = st.session_state[f"bulk_pname_{split_field}"]
                    if st.button("🚀 Upload Merged CSV", key=f"merged_ul_{split_field}", use_container_width=True):
                        ok, msg = upload_pipeline(
                            csv_bytes=st.session_state[merged_csv_key].encode("utf-8"),
                            filename=f"merged_selected_by_{_safe_name(split_field)}.csv",
                            token=token_m, project_id=int(project_id_m), project_name=project_name_m,
                            session=session, connect_timeout=connect_timeout, read_timeout=read_timeout)
                        (st.success if ok else st.error)(msg)
