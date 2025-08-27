import streamlit as st
import io
import json
import re
import pandas as pd
import math
from typing import List, Dict, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build

# python -m streamlit run app.py

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
# SERVICE_ACCOUNT_FILE = "turing-genai-ws-58339643dd3f.json"

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

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

@st.cache_resource
def authenticate_drive():
    if "gcp_service_account" in st.secrets:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        # local fallback ONLY for dev (keep file out of git)
        SERVICE_ACCOUNT_FILE = "turing-genai-ws-*.json"
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
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
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
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
# MULTI-IMAGE HELPERS (NEW)
# ─────────────────────────────────────────────────────────────────────────────
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.jfif', '.heic', '.svg']

def _has_image_ext(name: str) -> bool:
    name = (name or "").strip().lower()
    return any(name.endswith(ext) for ext in IMAGE_EXTENSIONS)

@st.cache_resource
def _drive_service_cached():
    # Use same auth path as elsewhere, but cache the built service for speed.
    return authenticate_drive()

@st.cache_data(show_spinner=False)
def mi_get_folder_name(folder_id: str) -> str:
    """Name lookup with its own tiny cache via st.cache_data."""
    svc = _drive_service_cached()
    try:
        meta = svc.files().get(
            fileId=folder_id,
            fields="id,name",
            supportsAllDrives=True
        ).execute()
        return meta.get("name", "")
    except Exception:
        return ""

@st.cache_data(show_spinner=False)
def mi_get_folder_details(folder_id: str) -> Dict[str, str]:
    """Return {name, parent} for a folder id."""
    svc = _drive_service_cached()
    try:
        meta = svc.files().get(
            fileId=folder_id,
            fields="id,name,parents",
            supportsAllDrives=True
        ).execute()
        return {
            "name": meta.get("name", "Unknown"),
            "parent": (meta.get("parents", [None]) or [None])[0]
        }
    except Exception:
        return {"name": "Unknown", "parent": None}

@st.cache_data(show_spinner=False)
def mi_build_complete_path(folder_id: str) -> str:
    """Build folder path from topmost parent to this folder."""
    parts, cur, seen = [], folder_id, set()
    while cur and cur not in seen:
        seen.add(cur)
        d = mi_get_folder_details(cur)
        if d["name"] and d["name"] != "Unknown":
            parts.insert(0, d["name"])
        cur = d["parent"]
        if not cur:
            break
    return "/".join(parts)

def _last_number(s: str) -> int:
    """Extract last integer in a string for natural page sorting; inf if none."""
    m = re.search(r'(\d+)(?!.*\d)', s or "")
    return int(m.group(1)) if m else 10**9

@st.cache_data(show_spinner=True)
def mi_list_images_with_paths(root_folder_id: str) -> pd.DataFrame:
    """
    Walk root folder (recursively), return one row per image with:
      image_name, image_link, folder_path, prompt, model_a, metadata (per-image)
    """
    svc = _drive_service_cached()
    stack = [(root_folder_id, mi_get_folder_name(root_folder_id))]
    rows = []

    while stack:
        fid, path = stack.pop()
        page_token = None

        while True:
            resp = svc.files().list(
                q=f"'{fid}' in parents and trashed=false",
                fields="nextPageToken, files(id,name,mimeType)",
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True
            ).execute()

            for it in resp.get("files", []):
                f_id   = it.get("id")
                name   = (it.get("name") or "").strip()
                mtype  = it.get("mimeType") or ""

                if mtype == "application/vnd.google-apps.folder":
                    stack.append((f_id, f"{path}/{name}" if path else name))
                    continue

                if mtype.startswith("image/") or _has_image_ext(name):
                    link = f"https://drive.google.com/file/d/{f_id}/view"
                    # Per-image metadata (kept simple—your downstream grouping will stack these)
                    meta = {
                        "image_name": name,
                        "image_link": link,
                        "capability": "multi-page" if "multi-page" in name.lower() else "single-page"
                    }
                    rows.append({
                        "image_name": name,
                        "image_link": link,
                        "folder_path": path,
                        "prompt": ".",
                        "model_a": ".",
                        "metadata": json.dumps(meta, ensure_ascii=False)
                    })

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    return pd.DataFrame(rows)

def mi_build_multi_metadata(group_df: pd.DataFrame, vendorsubdomain: str, vendorlanguage: str) -> str:
    """
    Given all pages in a folder, build a single JSON string with
    image_name_i / image_link_i keys (sorted by trailing number), plus static fields.
    """
    pages = group_df[["image_name", "image_link"]].dropna().astype(str).values.tolist()
    pages.sort(key=lambda x: _last_number(x[0]))

    meta = {}
    for i, (nm, ln) in enumerate(pages, start=1):
        meta[f"image_name_{i}"] = nm
        meta[f"image_link_{i}"] = ln

    meta["capability"] = "multi-page"
    meta["vendorsubdomain"] = vendorsubdomain
    meta["vendorlanguage"] = vendorlanguage
    meta["tasktype"] = "OCR"  # align with your sample

    return json.dumps(meta, ensure_ascii=False)



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

col1, col2, col3, col4, col5 = st.columns(5)
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

with col5:
    if st.button("🖼️ Multi Image (Drive)", use_container_width=True):
        st.session_state.mode = "Multi Image"
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
# MODE E: Multi Image (Drive)  (NEW)
# ─────────────────────────────────────────────────────────────────────────────
elif mode == "Multi Image":
    st.markdown('<div class="section-title">Scan Google Drive for Multi-Image Sets</div>', unsafe_allow_html=True)
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        folder_id = st.text_input("Root Google Drive Folder ID (images)")
        exclude_discarded = st.checkbox("Exclude language = Discarded_Scanned", value=True)
        st.markdown('</div>', unsafe_allow_html=True)

    if not folder_id:
        st.stop()

    try:
        with st.spinner("Scanning Drive recursively and building multi-page groups..."):
            per_image = mi_list_images_with_paths(folder_id)
        if per_image.empty:
            st.warning("No images found under this folder.")
            st.stop()

        # Derive vendorlanguage / vendorsubdomain from folder_path (same rules you showed)
        # language = 2nd segment; vendorsubdomain = second last segment
        def _lang_from_path(p):
            parts = (p or "").split("/")
            return parts[1] if len(parts) > 1 else ""

        def _subdomain_from_path(p):
            parts = (p or "").split("/")
            return parts[-2] if len(parts) >= 2 else ""

        per_image["vendorlanguage"]  = per_image["folder_path"].apply(_lang_from_path)
        per_image["vendorsubdomain"] = per_image["folder_path"].apply(_subdomain_from_path)

        # Optional filter: drop 'Discarded_Scanned'
        if exclude_discarded:
            per_image = per_image[per_image["vendorlanguage"].str.lower() != "discarded_scanned"]

        if per_image.empty:
            st.warning("No rows remain after filtering.")
            st.stop()

        # Normalize vendorsubdomain per your example mapping
        per_image["vendorsubdomain"] = per_image["vendorsubdomain"].replace({"English_en_AU": "printed"}).str.lower()

        # Group by folder to make one multi-image record per folder_path
        # Hand off RAW per-image rows; the new builder will do grouping & enumeration
        base_df = per_image
        available_fields = list(base_df.columns)
        source_type = "multi-image"

        st.success(
            f"Loaded {len(base_df)} images across {base_df['folder_path'].nunique()} folder(s). "
            "Use the Grouping section below to group by 'folder_path' (or any key)."
        )
        with st.expander("Preview per-image rows"):
            st.dataframe(base_df.head(20), use_container_width=True)


    except Exception as e:
        st.error(f"Multi-image error: {e}")
        st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# NEW: Dynamic Grouping + On-the-fly Metadata/Columns with Auto-Enumeration
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.markdown("### 🧩 Grouping & Output Builder (Dynamic)")

import uuid
import math

# Helpers
def _first_non_null(series: pd.Series):
    for v in series:
        if pd.notna(v) and str(v).strip() not in {"", "."}:
            return v
    return ""

def _join_unique_comma(series: pd.Series) -> str:
    vals = [str(v) for v in series if pd.notna(v) and str(v).strip() not in {"", "."}]
    uniq, seen = [], set()
    for v in vals:
        if v not in seen:
            uniq.append(v); seen.add(v)
    return ",".join(uniq)

_num_suffix_re = re.compile(r"(\d+)(?!.*\d)")
def _numeric_suffix(val: str) -> int:
    if not isinstance(val, str): return math.inf
    m = _num_suffix_re.search(val)
    return int(m.group(1)) if m else math.inf

def _snake(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "item"

def _enum_key(base: str, src: str, idx: int) -> str:
    """
    Base comes from user key; if empty, fall back to the selected source field.
    Always snake_case and suffix with _{idx}
    """
    head = _snake(base or src or "item")
    return f"{head}_{idx}"

# 1) Grouping controls
st.markdown("#### 1) Optional grouping")
g1, g2, g3 = st.columns([2, 2, 1])
with g1:
    group_keys = st.multiselect("Group by (choose one or more fields)",
                                options=available_fields, default=[])
with g2:
    sort_within_group_by = st.selectbox("Sort rows within group by",
                                        options=["(none)"] + available_fields, index=0)
with g3:
    use_numeric_suffix = st.checkbox("Numeric suffix sort", value=True,
                                     help="Great for page_1, page_2, …")

auto_enum_default = st.checkbox("Auto-enumerate metadata values when grouped", value=True)

# Session state for dynamic rows
if "meta_cfg" not in st.session_state:
    st.session_state.meta_cfg = []  # list of dicts: {id, key, source, static, enumerate}
if "col_cfg" not in st.session_state:
    st.session_state.col_cfg = []   # list of dicts: {id, name, source, static, agg}

# 2) Metadata UI (dynamic rows)
st.markdown("#### 2) Metadata entries")
rm_meta_ids = []
for cfg in st.session_state.meta_cfg:
    with st.container():
        c1, c2, c3, c4, c5 = st.columns([1.2, 1.4, 1.4, 1.0, 0.6])
        with c1:
            cfg["key"] = st.text_input("Key (optional)", value=cfg.get("key",""),
                                       key=f"meta_key_{cfg['id']}",
                                       placeholder="leave blank to use source field name")
        with c2:
            cfg["source"] = st.selectbox("Value source",
                                         options=["<STATIC>", "."] + available_fields,
                                         index=(["<STATIC>", "."] + available_fields).index(cfg.get("source","<STATIC>"))
                                                if cfg.get("source") in ["<STATIC>", "."] + available_fields else 0,
                                         key=f"meta_src_{cfg['id']}")
        with c3:
            if cfg["source"] == "<STATIC>":
                cfg["static"] = st.text_input("Static value", value=cfg.get("static",""),
                                              key=f"meta_static_{cfg['id']}")
            else:
                st.write("\n"); st.caption("Will read per-row/group from selected field")
                cfg["static"] = ""
        with c4:
            enum_suggested = bool(group_keys) and auto_enum_default and cfg["source"] not in {"<STATIC>", "."}
            cfg["enumerate"] = st.checkbox("Enumerate", value=cfg.get("enumerate", enum_suggested),
                                           key=f"meta_enum_{cfg['id']}",
                                           help="Create key_1, key_2, … across grouped items")
        with c5:
            if st.button("🗑", key=f"meta_rm_{cfg['id']}", help="Remove this metadata row"):
                rm_meta_ids.append(cfg["id"])
if rm_meta_ids:
    st.session_state.meta_cfg = [c for c in st.session_state.meta_cfg if c["id"] not in rm_meta_ids]

if st.button("➕ Add metadata"):
    st.session_state.meta_cfg.append({"id": uuid.uuid4().hex[:8],
                                      "key": "",
                                      "source": "<STATIC>",
                                      "static": "",
                                      "enumerate": True})

# Provide at least one row by default
if not st.session_state.meta_cfg:
    st.session_state.meta_cfg.append({"id": uuid.uuid4().hex[:8],
                                      "key": "",
                                      "source": "<STATIC>",
                                      "static": "",
                                      "enumerate": True})

# 3) Output columns UI (dynamic rows)
st.markdown("#### 3) Output columns")
rm_col_ids = []
for cfg in st.session_state.col_cfg:
    with st.container():
        c1, c2, c3, c4 = st.columns([1.2, 1.4, 1.2, 0.6])
        with c1:
            cfg["name"] = st.text_input("Column name", value=cfg.get("name",""),
                                        key=f"col_name_{cfg['id']}")
        with c2:
            cfg["source"] = st.selectbox("Value source",
                                         options=["<STATIC>", "."] + available_fields,
                                         index=(["<STATIC>", "."] + available_fields).index(cfg.get("source","."))
                                                if cfg.get("source") in ["<STATIC>", "."] + available_fields else 1,
                                         key=f"col_src_{cfg['id']}")
        with c3:
            if cfg["source"] == "<STATIC>":
                cfg["static"] = st.text_input("Static value", value=cfg.get("static",""),
                                              key=f"col_static_{cfg['id']}")
                cfg["agg"] = "first"
            else:
                cfg["static"] = ""
                if group_keys:
                    cfg["agg"] = st.selectbox("When grouped, aggregate via",
                                              options=["first", "join_unique_comma", "count"],
                                              index=["first","join_unique_comma","count"].index(cfg.get("agg","first")),
                                              key=f"col_agg_{cfg['id']}")
                else:
                    cfg["agg"] = "first"
                    st.selectbox("When grouped, aggregate via",
                                 options=["first"], index=0, key=f"col_agg_{cfg['id']}", disabled=True)
        with c4:
            if st.button("🗑", key=f"col_rm_{cfg['id']}", help="Remove this column"):
                rm_col_ids.append(cfg["id"])
if rm_col_ids:
    st.session_state.col_cfg = [c for c in st.session_state.col_cfg if c["id"] not in rm_col_ids]

if st.button("➕ Add column"):
    st.session_state.col_cfg.append({"id": uuid.uuid4().hex[:8],
                                     "name": "",
                                     "source": ".",
                                     "static": "",
                                     "agg": "first"})

# 4) Build output
def _order_group(df: pd.DataFrame) -> pd.DataFrame:
    if sort_within_group_by == "(none)":
        return df
    if use_numeric_suffix:
        tmp = df.copy()
        tmp["__ord"] = tmp[sort_within_group_by].astype(str).map(_numeric_suffix)
        tmp = tmp.sort_values(["__ord", sort_within_group_by]).drop(columns=["__ord"])
        return tmp
    return df.sort_values(sort_within_group_by)

def _build_metadata_row(row: pd.Series) -> dict:
    md = {}
    for m in st.session_state.meta_cfg:
        k, src = m.get("key",""), m.get("source",".")
        if src == "<STATIC>":
            base_key = _snake(k or "item")
            md[base_key] = m.get("static","")
        elif src in {".", None, ""}:
            base_key = _snake(k or "item")
            md[base_key] = "."
        else:
            base_key = _snake(k or src)
            # No grouping: no enumeration, just single value
            md[base_key] = get_value(row, src)
    return md

def _build_metadata_group(gdf: pd.DataFrame) -> dict:
    ordered = _order_group(gdf)
    md = {}
    for m in st.session_state.meta_cfg:
        k, src = m.get("key",""), m.get("source",".")
        enum = bool(m.get("enumerate", False))
        if src == "<STATIC>":
            base_key = _snake(k or "item")
            md[base_key] = m.get("static","")
            continue
        if src in {".", None, ""}:
            base_key = _snake(k or "item")
            md[base_key] = "."
            continue
        series_vals = [get_value(r, src) for _, r in ordered.iterrows()]
        series_vals = [v for v in series_vals if str(v).strip() not in {"", "."}]
        if enum or (auto_enum_default and series_vals and len(series_vals) > 1):
            # Auto-enumerate: image_name -> image_name_1, image_name_2, ...
            for i, v in enumerate(series_vals, start=1):
                md[_enum_key(k, src, i)] = v
        else:
            base_key = _snake(k or src)
            md[base_key] = _first_non_null(ordered[src])
    return md

def _col_value_row(row: pd.Series, cfg: dict):
    src = cfg.get("source",".")
    if src == "<STATIC>": return cfg.get("static","")
    if src in {".", None, ""}: return "."
    return get_value(row, src)

def _col_value_group(gdf: pd.DataFrame, cfg: dict):
    src, agg = cfg.get("source","."), cfg.get("agg","first")
    if src == "<STATIC>": return cfg.get("static","")
    if src in {".", None, ""}: return "."
    ordered = _order_group(gdf)
    if agg == "first": return _first_non_null(ordered[src])
    if agg == "join_unique_comma": return _join_unique_comma(ordered[src])
    if agg == "count": return int(ordered[src].notna().sum())
    return _first_non_null(ordered[src])

if st.button("Generate CSV"):
    out_rows = []
    if not group_keys:
        # One output row per base row
        for _, row in base_df.iterrows():
            md = _build_metadata_row(row)
            rec = {"metadata": json.dumps(md, ensure_ascii=False)}
            for cfg in st.session_state.col_cfg:
                name = (cfg.get("name") or "col").strip()
                rec[name] = _col_value_row(row, cfg)
            out_rows.append(rec)
    else:
        # One output row per group
        for _, gdf in base_df.groupby(group_keys, sort=False):
            md = _build_metadata_group(gdf)
            rec = {"metadata": json.dumps(md, ensure_ascii=False)}
            for cfg in st.session_state.col_cfg:
                name = (cfg.get("name") or "col").strip()
                rec[name] = _col_value_group(gdf, cfg)
            out_rows.append(rec)

    out_df_new = pd.DataFrame(out_rows)
    st.success(f"✅ Generated {len(out_df_new)} rows and {out_df_new.shape[1]} columns.")
    st.dataframe(out_df_new.head(50), use_container_width=True)

    csv_buf = io.StringIO()
    out_df_new.to_csv(csv_buf, index=False)
    st.download_button("⬇️ Download CSV",
                       data=csv_buf.getvalue(),
                       file_name="output.csv",
                       mime="text/csv")
