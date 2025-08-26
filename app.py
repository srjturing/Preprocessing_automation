import streamlit as st
import io
import json
import re
import pandas as pd
from typing import List, Dict, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build

# python -m streamlit run app.py

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

if "GOOGLE_APPLICATION_CREDENTIALS_JSON" in st.secrets:
    creds_dict = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    SERVICE_ACCOUNT_FILE = service_account.Credentials.from_service_account_info(creds_dict)
else:
    SERVICE_ACCOUNT_FILE = service_account.Credentials.from_service_account_file("turing-genai-ws-58339643dd3f.json")

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
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )
    return build("drive", "v3", credentials=creds)

def list_drive_images_recursive(service, parent_id: str, current_path=None, results=None) -> List[Dict]:
    """Recursively list images in a Drive folder tree with pagination."""
    if current_path is None:
        current_path = []
    if results is None:
        results = []

    query = f"'{parent_id}' in parents and trashed = false"
    page_token = None
    while True:
        resp = (
            service.files()
            .list(q=query, fields="nextPageToken, files(id, name, mimeType)", pageToken=page_token)
            .execute()
        )
        files = resp.get("files", [])

        for file in files:
            mime = file.get("mimeType", "")
            if mime == "application/vnd.google-apps.folder":
                list_drive_images_recursive(
                    service, file["id"], current_path + [file["name"]], results
                )
            elif mime.startswith("image/"):
                results.append(
                    {
                        "image_name": file["name"],
                        "image_link": f"https://drive.google.com/file/d/{file['id']}/view",
                        "full_path": current_path + [file["name"]],
                    }
                )

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
st.set_page_config(page_title="Flexible Output CSV Builder", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)
st.title("📦 Flexible Output CSV Builder")

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
    st.markdown('<div class="section-help">First, enter how many output columns you want and how many metadata key–value pairs to include.</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        n_cols_input = st.number_input(
            "Number of output columns (1st is always metadata)",
            min_value=1, max_value=50, value=4, step=1, format="%d"
        )
    with c2:
        meta_count_input = st.number_input(
            "Number of metadata fields",
            min_value=0, max_value=50, value=2, step=1, format="%d"
        )
    counts_submitted = st.form_submit_button("Continue")

if counts_submitted:
    # Lock in counts; selectors will be shown next
    st.session_state.n_cols = int(n_cols_input)
    st.session_state.meta_count = int(meta_count_input)
    st.session_state.counts_confirmed = True
    st.toast("Counts confirmed. Configure fields below.", icon="✅")

# If not confirmed yet, stop here (no selectors visible)
if not st.session_state.counts_confirmed:
    st.stop()

n_cols = st.session_state.n_cols
meta_count = st.session_state.meta_count

# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT CONFIG: STEP 2 — FIELD SELECTIONS (UNCHANGED)
# ─────────────────────────────────────────────────────────────────────────────
output_colnames_fixed = [
    "metadata",
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
    st.markdown('<div class="section-help">Now choose the metadata pairs and map each output column to a source field.</div>', unsafe_allow_html=True)

    # Metadata pairs
    metadata_pairs = []
    if meta_count > 0:
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

    # Columns 2..N
    chosen_output_cols = ["metadata"]
    chosen_sources = {"metadata": None}

    if n_cols > 1:
        st.markdown("**Output columns**")
        for j in range(2, n_cols + 1):
            c1, c2 = st.columns([1, 1])
            with c1:
                colname = st.selectbox(
                    f"Name for output column #{j}",
                    options=[x for x in output_colnames_fixed if x != "metadata"],
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
    # Construct metadata JSON per row
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

    out_df = pd.DataFrame()
    out_df["metadata"] = base_df.apply(build_metadata, axis=1)

    # Add other chosen columns based on mapping; default '.' when field missing
    for name in chosen_output_cols[1:]:
        src_field = chosen_sources.get(name)
        if src_field is None:
            out_df[name] = "."
        else:
            out_df[name] = base_df.apply(lambda r: get_value(r, src_field), axis=1)

    st.success(f"✅ Generated {len(out_df)} rows and {out_df.shape[1]} columns.")
    st.dataframe(out_df.head(20), use_container_width=True)

    csv_buf = io.StringIO()
    out_df.to_csv(csv_buf, index=False)
    st.download_button(
        "⬇️ Download CSV",
        data=csv_buf.getvalue(),
        file_name="output.csv",
        mime="text/csv",
    )
