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

# python -m streamlit run app.py

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
#SERVICE_ACCOUNT_FILE = "turing-genai-ws-58339643dd3f.json" 

# ─────────────────────────────────────────────────────────────────────────────
# STYLES
# ─────────────────────────────────────────────────────────────────────────────
CSS = """
<style>
/* Page width & typography tweaks */
.block-container { padding-top: 1.2rem; padding-bottom: 3rem; }
:root { --radius: 14px; }
h1, h2, h3 { letter-spacing: 0.2px; }

/* Buttons row */
.mode-btn {
  width: 100%;
  border-radius: 12px;
  padding: 0.9rem 0.75rem;
  font-weight: 600;
  border: 1px solid rgba(0,0,0,0.08);
  background: #f8f9fb;
}
.mode-btn:hover { background: #eef1f6; }

/* Section headers */
.section-title {
  font-size: 1.05rem;
  font-weight: 700;
  margin-bottom: 0.35rem;
}
.section-help {
  color: #5f6c7b; font-size: 0.9rem; margin-bottom: 0.6rem;
}

/* Small labels spacing */
label, .stRadio label, .stSelectbox label, .stNumberInput label { font-weight: 600; }
</style>
"""

# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE DRIVE HELPERS (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def authenticate_drive():
    """
    Authenticate with longer HTTP timeouts (5 minutes) and discovery cache off.
    Uses google_auth_httplib2.AuthorizedHttp (not creds.authorize()).
    """
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        SERVICE_ACCOUNT_FILE = "turing-genai-ws-58339643dd3f.json"
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=300))  # 5-minute read timeout
    return build("drive", "v3", http=http, cache_discovery=False)


def list_drive_images_recursive(service, parent_id: str, current_path=None, results=None) -> List[Dict]:
    """Recursively list images in a Drive folder tree with robust retries & pagination."""
    if current_path is None:
        current_path = []
    if results is None:
        results = []

    query = (
        f"'{parent_id}' in parents and trashed = false and "
        "(mimeType = 'application/vnd.google-apps.folder' or mimeType contains 'image/')"
    )
    page_token = None
    max_retries = 5
    backoff = 1.5

    while True:
        req = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                pageSize=1000,              # fewer API calls
                corpora="allDrives",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
        )

        attempt = 0
        while True:
            try:
                resp = req.execute(num_retries=3)
                break
            except Exception:
                attempt += 1
                if attempt >= max_retries:
                    raise
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
        if not page_token:
            break

    return results


def build_drive_dataframe(folder_id: str) -> Tuple[pd.DataFrame, List[str]]:
    """Return (drive_df, available_fields_for_drive)."""
    service = authenticate_drive()
    image_data = list_drive_images_recursive(service, folder_id)
    if not image_data:
        return pd.DataFrame(), []

    drive_df = pd.DataFrame(image_data)
    # Build level_* columns
    max_depth = max(len(p) for p in drive_df["full_path"])
    for i in range(max_depth):
        drive_df[f"level_{i}"] = drive_df["full_path"].apply(lambda x: x[i] if i < len(x) else "")
    drive_df["path_preview"] = drive_df["full_path"].apply(lambda x: "/".join(x))
    # Available fields: image_name + all levels + image_link + path_preview
    level_cols = [f"level_{i}" for i in range(max_depth)]
    available_fields = ["image_name"] + level_cols + ["image_link", "path_preview"]
    return drive_df, available_fields

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING (CSV / EXCEL) — UNCHANGED
# ─────────────────────────────────────────────────────────────────────────────
def load_tabular(uploaded_file) -> Tuple[pd.DataFrame, List[str]]:
    """Load CSV/XLS/XLSX into DataFrame. Returns (df, columns)."""
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

# Common helper: safe getter with default '.'
def get_value(row: pd.Series, field: str) -> str:
    if field == ".":
        return "."
    return str(row.get(field, "."))

def reset_counts():
    st.session_state.counts_confirmed = False
    st.session_state.n_cols = None
    st.session_state.meta_count = None

# ─────────────────────────────────────────────────────────────────────────────
# JSON HELPERS (NEW)
# ─────────────────────────────────────────────────────────────────────────────
def _flatten_leaves(d: dict, parent_prefix: str) -> dict:
    """
    Return only leaf keys as hyphen-joined paths (e.g., a-b-c).
    Non-leaf (dict) nodes are never emitted as values.
    """
    out = {}
    for k, v in d.items():
        key_path = f"{parent_prefix}-{k}" if parent_prefix else k
        if isinstance(v, dict):
            out.update(_flatten_leaves(v, key_path))
        else:
            out[key_path] = v
    return out

def build_json_dataframe(uploaded_json_file) -> Tuple[pd.DataFrame, List[str]]:
    """
    Parse uploaded JSON (top-level fileMetadata + workitems[]). Rules:
      - Only deepest leaf keys become fields.
      - Paths use hyphen separators (e.g., workitems-workItemId, workitems-inputData-desc).
      - Collapse inputData keys named Image_1, Image_2, … into a single
        workitems-inputData-Image with comma-separated values.
      - fileMetadata-* keys are applied to every row so users may include them.
    Returns (df, available_fields).
    """
    # Streamlit's UploadedFile is binary; decode and json.load from a text buffer
    text_fp = io.StringIO(uploaded_json_file.getvalue().decode("utf-8"))
    data = json.load(text_fp)

    file_meta = data.get("fileMetadata", {})
    file_meta_flat = {f"fileMetadata-{k}": v for k, v in file_meta.items()}

    rows = []
    for wi in data.get("workitems", []):
        row = {}

        # Special case: workItemId → workitems-workItemId
        if "workItemId" in wi:
            row["workitems-workItemId"] = wi["workItemId"]

        for k, v in wi.items():
            if k == "workItemId":
                continue
            if isinstance(v, dict):
                if k == "inputData":
                    # Collapse Image_# keys and flatten other leaves
                    images = []
                    for ik, iv in v.items():
                        if re.fullmatch(r"Image_\d+", ik):
                            images.append(iv)
                        elif isinstance(iv, dict):
                            # Nested dicts under inputData
                            nested = _flatten_leaves(iv, f"workitems-{k}")
                            row.update(nested)
                        else:
                            row[f"workitems-{k}-{ik}"] = iv
                    if images:
                        row["workitems-inputData-Image"] = ", ".join(images)
                else:
                    # Generic nested dicts directly under workitems
                    row.update(_flatten_leaves(v, f"workitems-{k}"))
            else:
                # Non-dict leaves under workitems
                row[f"workitems-{k}"] = v

        # Attach file-level metadata on every row (optional for output)
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

st.markdown(
    """
<div class="card">
  <div class="section-title">Choose your input mode</div>
  <div class="section-help">Start by selecting how you'd like to provide data.</div>
</div>
""",
    unsafe_allow_html=True,
)
st.divider()

# — Mode selection (now FOUR modes)
if "mode" not in st.session_state:
    st.session_state.mode = None
    reset_counts()

col1, col2, col3, col4 = st.columns(4)
with col1:
    if st.button("📄 CSV / Excel", use_container_width=True):
        st.session_state.mode = "CSV/Excel"
        reset_counts()
with col2:
    if st.button("🗂️ Drive Folder ID", use_container_width=True):
        st.session_state.mode = "Drive Folder ID"
        reset_counts()
with col3:
    if st.button("🔗 Both (CSV + Drive)", use_container_width=True):
        st.session_state.mode = "Both (CSV + Drive)"
        reset_counts()
with col4:
    if st.button("🧾 JSON file", use_container_width=True):
        st.session_state.mode = "JSON"
        reset_counts()

mode = st.session_state.mode
if not mode:
    st.stop()

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# MODE A: CSV/Excel only (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
if mode == "CSV/Excel":
    st.markdown('<div class="section-title">Upload CSV / Excel</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        uploaded_file = st.file_uploader("Upload .csv, .xlsx, or .xls", type=["csv", "xlsx", "xls"])
        st.markdown('</div>', unsafe_allow_html=True)

    if not uploaded_file:
        st.stop()

    try:
        base_df, csv_cols = load_tabular(uploaded_file)
        available_fields = csv_cols[:]  # from CSV/Excel columns
        st.success(f"Loaded {len(base_df)} rows from the uploaded file.")
        st.dataframe(base_df.head(20), use_container_width=True)
        source_type = "csv"
    except Exception as e:
        st.error(f"Failed to read the uploaded file: {e}")
        st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# MODE B: Drive Folder only (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
elif mode == "Drive Folder ID":
    st.markdown('<div class="section-title">Enter Google Drive Folder ID</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        folder_id = st.text_input("Google Drive Folder ID (images)")
        st.markdown('</div>', unsafe_allow_html=True)

    if not folder_id:
        st.stop()

    try:
        with st.spinner("Scanning Google Drive folder recursively..."):
            drive_df, drive_fields = build_drive_dataframe(folder_id)
        if drive_df.empty:
            st.warning("No images found in the provided Drive folder (including subfolders).")
            st.stop()
        base_df = drive_df
        available_fields = drive_fields[:]  # level_* + image_link + path_preview + image_name
        st.success(f"Discovered {len(base_df)} images.")
        st.dataframe(base_df[["image_name", "path_preview", "image_link"]].head(20), use_container_width=True)
        source_type = "drive"
    except Exception as e:
        st.error(f"Drive error: {e}")
        st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# MODE C: Both (CSV + Drive) (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
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
        st.info("Provide both a CSV/Excel file and a Drive Folder ID.")
        st.stop()

    # Load both sources
    try:
        csv_df, csv_cols = load_tabular(uploaded_file)
    except Exception as e:
        st.error(f"Failed to read the uploaded file: {e}")
        st.stop()

    try:
        with st.spinner("Scanning Google Drive folder recursively..."):
            drive_df, drive_fields = build_drive_dataframe(folder_id)
        if drive_df.empty:
            st.warning("No images found in the provided Drive folder (including subfolders).")
            st.stop()
    except Exception as e:
        st.error(f"Drive error: {e}")
        st.stop()

    st.success(f"CSV rows: {len(csv_df)} • Drive images: {len(drive_df)}")

    # Show what keys we have on each side
    with st.expander("📄 CSV Columns"):
        st.write(csv_cols)
        st.dataframe(csv_df.head(10), use_container_width=True)
    with st.expander("🗂️ Drive Fields (paths & links)"):
        st.write(drive_fields)
        st.dataframe(drive_df[["image_name", "path_preview", "image_link"]].head(10), use_container_width=True)

    st.markdown("### 🔗 Choose Mapping Keys")
    st.caption("Pick one column from CSV and one field from Drive. These act as unique keys to merge.")
    map_col_csv = st.selectbox("CSV key column", options=csv_cols, index=0)
    map_col_drive = st.selectbox("Drive key field", options=drive_fields, index=0)

    norm_col = st.checkbox("Normalize keys (strip & lowercase) before merging", value=True)

    # Prepare temporary key columns for reliable merge
    def _normalize_series(s: pd.Series) -> pd.Series:
        s = s.astype(str)
        if norm_col:
            return s.str.strip().str.lower()
        return s

    csv_key = _normalize_series(csv_df[map_col_csv])
    drive_key = _normalize_series(drive_df[map_col_drive])

    # Warn for duplicates on either side
    dup_csv = csv_key.duplicated(keep=False).sum()
    dup_drive = drive_key.duplicated(keep=False).sum()
    if dup_csv > 0:
        st.warning(f"CSV mapping key has {dup_csv} duplicate entries. Merge may not be 1:1.")
    if dup_drive > 0:
        st.warning(f"Drive mapping key has {dup_drive} duplicate entries. Merge may not be 1:1.")

    # Build prefixed copies to avoid name collisions and to expose both sides in output
    csv_prefixed = csv_df.copy()
    csv_prefixed.columns = [f"csv:{c}" for c in csv_prefixed.columns]
    drive_prefixed = drive_df.copy()
    drive_prefixed.columns = [f"drive:{c}" for c in drive_prefixed.columns]

    # Add normalized join keys
    csv_prefixed["_join_key"] = csv_key.values
    drive_prefixed["_join_key"] = drive_key.values

    # Merge (inner join keeps only matches; offer a selector)
    join_how = st.selectbox("Merge strategy", options=["inner", "left", "right", "outer"], index=0)
    merged = pd.merge(csv_prefixed, drive_prefixed, on="_join_key", how=join_how)

    if merged.empty:
        st.error("Merged result is empty. Try a different mapping key or merge strategy.")
        st.stop()

    st.success(f"Merged rows: {len(merged)} (join={join_how})")
    with st.expander("Preview merged data"):
        st.dataframe(merged.head(20), use_container_width=True)

    # Expose all fields from both sides for output configuration
    base_df = merged.drop(columns=["_join_key"])
    available_fields = list(base_df.columns)
    source_type = "both"

# ─────────────────────────────────────────────────────────────────────────────
# MODE D: JSON file (NEW)
# ─────────────────────────────────────────────────────────────────────────────
elif mode == "JSON":
    st.markdown('<div class="section-title">Upload JSON</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        uploaded_json = st.file_uploader("Upload .json", type=["json"])
        st.markdown('</div>', unsafe_allow_html=True)

    if not uploaded_json:
        st.stop()

    try:
        with st.spinner("Parsing JSON and extracting deepest-leaf fields..."):
            df_json, json_fields = build_json_dataframe(uploaded_json)
        if df_json.empty:
            st.warning("JSON contained no workitems.")
            st.stop()

        base_df = df_json
        available_fields = json_fields[:]  # expose fileMetadata-* and workitems-* leaves
        source_type = "json"

        st.success(f"Parsed {len(base_df)} workitems from JSON.")
        st.dataframe(base_df.head(20), use_container_width=True)

        with st.expander("Available fields from JSON"):
            st.write(available_fields)
    except Exception as e:
        st.error(f"JSON parse error: {e}")
        st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# METADATA TOGGLE (NEW)
# ─────────────────────────────────────────────────────────────────────────────
if "include_metadata" not in st.session_state:
    st.session_state.include_metadata = True  # default ON to match previous behavior

# Prefer st.toggle if your Streamlit version supports it; fallback to checkbox otherwise:
try:
    st.session_state.include_metadata = st.toggle(
        "Metadata needed?",
        value=st.session_state.include_metadata,
        help="Turn OFF if you don't want a 'metadata' JSON column in the output."
    )
except Exception:
    st.session_state.include_metadata = st.checkbox(
        "Metadata needed?",
        value=st.session_state.include_metadata,
        help="Turn OFF if you don't want a 'metadata' JSON column in the output."
    )


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT CONFIG: STEP 1 — ENTER COUNTS (numbers only) (UNCHANGED)
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
        if st.session_state.include_metadata:
            label += " (1st is metadata)"
        else:
            label += " (no metadata column)"
        n_cols_input = st.number_input(
            label,
            min_value=1, max_value=50, value=4, step=1, format="%d"
        )

    with c2:
        if st.session_state.include_metadata:
            meta_count_input = st.number_input(
                "Number of metadata fields",
                min_value=0, max_value=50, value=2, step=1, format="%d"
            )
        else:
            # Hide the widget and force 0 when metadata is OFF
            st.empty()
            meta_count_input = 0

    counts_submitted = st.form_submit_button("Continue")


if counts_submitted:
    # Lock in counts; selectors will be shown next
    st.session_state.n_cols = int(n_cols_input)
    st.session_state.meta_count = int(meta_count_input) if st.session_state.include_metadata else 0
    st.session_state.counts_confirmed = True
    st.toast("Counts confirmed. Configure fields below.", icon="✅")


# If not confirmed yet, stop here (no selectors visible)
if not st.session_state.counts_confirmed:
    st.stop()

n_cols = st.session_state.n_cols
meta_count = st.session_state.meta_count

# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT CONFIG: STEP 2 — FIELD SELECTIONS (UPDATED)
# ─────────────────────────────────────────────────────────────────────────────
# Keep "metadata" in the fixed list for naming convenience, but we won’t force it
output_colnames_fixed = [
    "metadata",
    "workitem_id",
    "image",
    "image_name",
    "image_link",
    "prompt",
    "question",
    "model_a",
    "model_b",
    "file_path",
    "video_name",
]

with st.form("output_config_form"):
    st.markdown('<div class="section-help">Now choose the metadata pairs (if enabled) and map each output column to a source field.</div>', unsafe_allow_html=True)

    # --- Metadata pairs (only if toggle is ON) ---
    metadata_pairs = []
    if st.session_state.include_metadata and meta_count > 0:
        st.markdown("**Metadata (JSON) fields**")
        for i in range(meta_count):
            c1, c2 = st.columns([1, 1])
            with c1:
                key_name = st.text_input(f"Metadata key {i+1}", key=f"meta_key_{i}")
            with c2:
                src_options = ["."] + available_fields
                value_field = st.selectbox(
                    f"Value for key {i+1}",
                    options=src_options,
                    key=f"meta_val_{i}",
                )
            if key_name:
                metadata_pairs.append((key_name, value_field))

    # --- Columns selection ---
    chosen_output_cols = []
    chosen_sources = {}

    # If metadata is included, reserve the first column
    start_idx = 1
    if st.session_state.include_metadata:
        chosen_output_cols = ["metadata"]
        chosen_sources["metadata"] = None
        start_idx = 2  # subsequent columns start from #2

    if n_cols >= start_idx:
        st.markdown("**Output columns**")
        # Build the options list WITHOUT forcing "metadata"
        name_options = [x for x in output_colnames_fixed if x != "metadata"]

        for j in range(start_idx, n_cols + 1):
            c1, c2 = st.columns([1, 1])
            with c1:
                colname = st.selectbox(
                    f"Name for output column #{j}",
                    options=name_options,
                    key=f"out_col_{j}",
                )
            with c2:
                src = st.selectbox(
                    f"Data for '{colname}'",
                    options=["."] + available_fields,
                    key=f"out_src_{j}",
                )
            chosen_output_cols.append(colname)
            chosen_sources[colname] = src

    submitted = st.form_submit_button("Generate CSV")


if submitted:
    out_df = pd.DataFrame()

    # Construct metadata JSON per row (only if enabled)
    if st.session_state.include_metadata:
        def build_metadata(row: pd.Series) -> str:
            if meta_count == 0 or len(metadata_pairs) == 0:
                return "{}"
            md = {}
            for k, field in metadata_pairs:
                md[k] = get_value(row, field)
            try:
                return json.dumps(md, ensure_ascii=False)
            except Exception:
                return json.dumps({k: str(v) for k, v in md.items()}, ensure_ascii=False)

        out_df["metadata"] = base_df.apply(build_metadata, axis=1)

    # Add other chosen columns based on mapping; default '.' when field missing
    # Note: chosen_output_cols may or may not include 'metadata' depending on the toggle
    for name in chosen_output_cols:
        if name == "metadata":
            continue  # already added if enabled
        src_field = chosen_sources.get(name)
        if src_field is None:
            out_df[name] = "."
        else:
            out_df[name] = base_df.apply(lambda r: get_value(r, src_field), axis=1)

    st.success(f"✅ Generated {len(out_df)} rows and {out_df.shape[1]} columns.")
    st.dataframe(out_df.head(20), use_container_width=True)

    # Persist output for post-submit interactions (avoid disappearing UI on rerun)
    st.session_state["out_df"] = out_df
    st.session_state["available_fields_for_split"] = available_fields
    st.session_state["base_df_for_split"] = base_df
    # Keep last selection across reruns
    if "split_field" not in st.session_state:
        st.session_state["split_field"] = "None"

# ─────────────────────────────────────────────────────────────────────────────
# PERSISTENT: Split & Download (final)
# ─────────────────────────────────────────────────────────────────────────────
if "out_df" in st.session_state:
    st.markdown("#### ➗ Split & Download")
    out_df = st.session_state["out_df"]
    available_fields = st.session_state["available_fields_for_split"]
    base_df = st.session_state["base_df_for_split"]

    # Helper
    def _safe_name(s: str) -> str:
        s = str(s) if s is not None else "UNSPECIFIED"
        s = s.strip() or "UNSPECIFIED"
        s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
        return s[:80]

    # Choose field to split by (field names; "None" means no split)
    split_options = ["None"] + sorted(list(dict.fromkeys(map(str, available_fields))))
    default_split = st.session_state.get("split_field", "None")
    if default_split not in split_options:
        default_split = "None"

    split_field = st.selectbox(
        "Split by (choose a field from your input). If 'None', a single CSV is produced.",
        options=split_options,
        index=split_options.index(default_split),
        key="split_field",
    )

    # ── SINGLE CSV PATH (no split): build immediately and STOP rendering further UI
    is_none = str(split_field).strip().lower() == "none"
    if is_none:
        # Clear any previous split artifacts so stale widgets don't stick around
        for k in list(st.session_state.keys()):
            if k.startswith("__selected_groups__") or k.startswith("__merged_selected_csv__") \
               or k.startswith("__show_merged_dl__"):
                del st.session_state[k]

        # Build once; cache by a light signature (rows + columns)
        single_csv_bytes_key = "__single_csv_bytes__"
        single_csv_sig_key = "__single_csv_sig__"
        sig = (len(out_df), tuple(out_df.columns))

        if st.session_state.get(single_csv_sig_key) != sig:
            buf = io.StringIO()
            out_df.to_csv(buf, index=False)
            st.session_state[single_csv_bytes_key] = buf.getvalue()
            st.session_state[single_csv_sig_key] = sig

        st.download_button(
            "⬇️ Download CSV",
            data=st.session_state[single_csv_bytes_key],
            file_name="output.csv",
            mime="text/csv",
            key="single_csv_dl",
            use_container_width=True,
        )
        st.stop()  # IMPORTANT: prevent the split UI below from rendering

    # ── SPLIT PATH (field selected)
    series = base_df.get(split_field)
    if series is None:
        st.error("Selected split field not found in the input data.")
        st.stop()

    # Build groups once (no CSV conversion here)
    groups = {}
    for idx, val in series.items():
        key_val = _safe_name(val if pd.notna(val) else "UNSPECIFIED")
        groups.setdefault(key_val, []).append(idx)

    # Session keys (scoped per split field)
    sel_state_key      = f"__selected_groups__{split_field}"         # {gval: bool}
    merged_csv_key     = f"__merged_selected_csv__{split_field}"      # str
    show_merged_dl_key = f"__show_merged_dl__{split_field}"           # bool

    st.session_state.setdefault(sel_state_key, {})
    st.session_state.setdefault(merged_csv_key, None)
    st.session_state.setdefault(show_merged_dl_key, False)

    st.markdown("**Groups**")

    # Render group rows: [checkbox]  [label + size]  [Generate CSV (single-click download)]
    # No bulk-select row anymore.
    selected_count = 0
    for gval, idxs in sorted(groups.items(), key=lambda x: x[0].lower()):
        c0, c1, c2 = st.columns([0.08, 0.62, 0.30])
        with c0:
            checked = st.checkbox(
                "",
                value=st.session_state[sel_state_key].get(gval, False),
                key=f"sel_{split_field}_{gval}",
            )
            st.session_state[sel_state_key][gval] = bool(checked)
            if checked:
                selected_count += 1
        with c1:
            st.write(f"**{gval}** — {len(idxs)} rows")
        with c2:
            # Single-click generate & download (no separate generate step)
            subset_csv = out_df.loc[idxs].to_csv(index=False)
            st.download_button(
                "⬇️ Generate CSV",
                data=subset_csv,
                file_name=f"output_{_safe_name(split_field)}_{gval}.csv",
                mime="text/csv",
                key=f"dl_{split_field}_{gval}",
                use_container_width=True,
            )

    st.markdown("---")
    st.write(f"Selected groups: **{selected_count} / {len(groups)}**")

    # Bottom actions: Download ALL as ZIP and Apply selection (merges selected)
    c1, c2 = st.columns([0.5, 0.5])

    # Download ALL as ZIP — immediate compute & download
    with c1:
        zip_bytes = io.BytesIO()
        with zipfile.ZipFile(zip_bytes, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for gval, idxs in groups.items():
                subset_csv = out_df.loc[idxs].to_csv(index=False)
                zf.writestr(f"output_{_safe_name(split_field)}_{gval}.csv", subset_csv)
        zip_bytes.seek(0)
        st.download_button(
            f"📦 Download ALL as ZIP ({len(groups)} files)",
            data=zip_bytes.getvalue(),
            file_name=f"output_by_{_safe_name(split_field)}.zip",
            mime="application/zip",
            key=f"zip_all_dl_{split_field}",
            use_container_width=True,
        )

    # Apply selection now performs the merge and reveals a "Download Selected" button
    with c2:
        if st.button("✅ Apply selection"):
            chosen = [g for g, v in st.session_state[sel_state_key].items() if v and g in groups]
            frames = [out_df.loc[groups[g]] for g in chosen]
            merged_df = pd.concat(frames, axis=0, ignore_index=True) if frames else out_df.iloc[0:0]
            st.session_state[merged_csv_key] = merged_df.to_csv(index=False)
            st.session_state[show_merged_dl_key] = True
            st.toast("Selection applied. Merged CSV is ready.", icon="✅")

    # Show the Download Selected button after Apply selection
    if st.session_state.get(show_merged_dl_key) and st.session_state.get(merged_csv_key):
        st.download_button(
            f"⬇️ Download Selected (Merged CSV)",
            data=st.session_state[merged_csv_key],
            file_name=f"merged_selected_by_{_safe_name(split_field)}.csv",
            mime="text/csv",
            key=f"merged_selected_dl_{split_field}",
            use_container_width=True,
        )
