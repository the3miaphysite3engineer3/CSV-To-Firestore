import streamlit as st
import pandas as pd
from google.cloud import firestore
import math
import time
from typing import Dict, Any


# ---------------- Configuration ----------------
COLLECTION_NAME = "attendance"
ROWS_PER_PAGE = 10
CSV_FILE_PATH = "George_edited.csv"

ATTENDED_COLUMN = "Attended"
NAME_COLUMN = "Name"

# ---------------- Firestore Connection ----------------
@st.cache_resource
def get_firestore_db():
    """Initializes and returns the Firestore client."""
    try:
        if "firestore" in st.secrets:
            db = firestore.Client.from_service_account_info(st.secrets["firestore"])
            st.sidebar.success("Connected to Firestore via Streamlit secrets.")
        else:
            db = firestore.Client.from_service_account_json("firestore-key.json")
            st.sidebar.success("Connected to Firestore via local firestore-key.json.")
    except Exception as e:
        st.sidebar.error(
            "Could not connect to Firestore. Provide credentials via st.secrets['firestore'] or firestore-key.json."
        )
        st.stop()
    return db

db = get_firestore_db()

# ---------------- Utilities ----------------
def clean_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are completely empty and trim column names."""
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df

# ---------------- Migration: CSV --> Firestore ----------------
def migrate_csv_to_firestore():
    st.info(f"Starting migration from `{CSV_FILE_PATH}` to collection: `{COLLECTION_NAME}`")
    try:
        df = pd.read_csv(CSV_FILE_PATH, encoding="utf-8")
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        return

    df = clean_csv(df)

    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    data = df.to_dict("records")
    total_records = len(data)
    if total_records == 0:
        st.warning("No records found in CSV after cleaning.")
        return

    collection_ref = db.collection(COLLECTION_NAME)
    batch = db.batch()
    batch_counter = 0
    record_counter = 0

    progress_bar = st.progress(0.0)
    status_text = st.empty()

    for record in data:
        # remove NaN values so Firestore doesn't get them
        cleaned_record = {str(k).strip(): v for k, v in record.items() if pd.notna(v)}
        # Optionally convert attended-like values to canonical strings:
        if ATTENDED_COLUMN in cleaned_record:
            val = str(cleaned_record[ATTENDED_COLUMN]).strip().lower()
            cleaned_record[ATTENDED_COLUMN] = "Yes" if val in ["yes", "نعم", "true", "1"] else "No"

        doc_ref = collection_ref.document(document_id=str(cleaned_record["ID"]))  # auto-id
        batch.set(doc_ref, cleaned_record)

        batch_counter += 1
        record_counter += 1

        # Firestore batch limit is 500
        if batch_counter >= 500:
            batch.commit()
            batch = db.batch()
            batch_counter = 0
            progress_bar.progress(record_counter / total_records)
            status_text.text(f"Migrated {record_counter}/{total_records} records...")
            time.sleep(0.05)

    # commit remaining
    if batch_counter > 0:
        batch.commit()

    progress_bar.progress(1.0)
    status_text.text(f"Migrated {record_counter}/{total_records} records.")
    st.success(f"Migration complete! {record_counter} records uploaded to Firestore.")
    # Clear cached loader so UI refreshes
    load_data.clear()
    # rerun to reflect data
    st.rerun()

# ---------------- Loading Data from Firestore ----------------
@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    """Load all docs from Firestore collection into a DataFrame indexed by doc id."""
    docs_list = []
    try:
        docs = db.collection(COLLECTION_NAME).order_by("ID", direction=firestore.Query.ASCENDING).stream()
    except Exception as e:
        st.error(f"Error reading Firestore collection: {e}")
        return pd.DataFrame()

    for doc in docs:
        record = doc.to_dict()
        record["_doc_id"] = str(doc.id)
        docs_list.append(record)

    if not docs_list:
        return pd.DataFrame()

    df = pd.DataFrame(docs_list)
    df = df.set_index("_doc_id", drop=True)
    return df

# ---------------- Update Firestore from data_editor changes ----------------
def update_firestore_record():
    """Reads edited_rows from st.session_state['data_editor_key'] and updates Firestore documents."""
    editor_state = st.session_state.get("data_editor_key", {})
    edited_rows = editor_state.get("edited_rows", {}) or {}

    # Also consider edited_cells (some streamlit versions prefer edited_cells)
    # Convert edited_cells to edited_rows mapping if necessary
    if not edited_rows and "edited_cells" in editor_state and editor_state["edited_cells"]:
        # Build edited_rows from edited_cells
        tmp: Dict[str, Dict[str, Any]] = {}
        for cell in editor_state["edited_cells"]:
            # cell layout: {"row": <int>, "column": "<colname>", "value": <new value>}
            row = str(cell.get("row"))
            col = cell.get("column")
            val = cell.get("value")
            tmp.setdefault(row, {})[col] = val
        edited_rows = tmp

    if not edited_rows:
        st.info("No changes detected.")
        return

    # Load the full df and determine current page slice to map page-row -> doc_id
    full_df = load_data()
    if full_df.empty:
        st.warning("No data available to map changes to Firestore.")
        return

    # Ensure page_number exists in session state
    page_number = st.session_state.get("page_number", 1)
    total_rows = len(full_df)
    total_pages = max(1, math.ceil(total_rows / ROWS_PER_PAGE))
    page_number = max(1, min(page_number, total_pages))

    start_idx = (page_number - 1) * ROWS_PER_PAGE
    end_idx = min(start_idx + ROWS_PER_PAGE, total_rows)
    page_df = full_df.iloc[start_idx:end_idx].copy()

    batch = db.batch()
    update_count = 0
    failed = 0

    # edited_rows keys may be str or int representing the row position within the displayed page (0-based)
    for row_pos_key, changes in edited_rows.items():
        try:
            row_pos_int = int(row_pos_key)
        except Exception:
            # sometimes keys are tuples or other; skip if cannot interpret
            st.warning(f"Skipping unknown edited row key: {row_pos_key}")
            continue

        # Map the row position in the page result to the actual doc id (index)
        if row_pos_int < 0 or row_pos_int >= len(page_df):
            st.warning(f"Edited row {row_pos_int} is outside the current page range; skipping.")
            continue

        real_doc_id = str(page_df.index[row_pos_int])

        # Prepare changes dictionary - convert Attended boolean to canonical string
        changes_to_write = dict(changes)  # copy
        if ATTENDED_COLUMN in changes_to_write:
            # Accept True/False or yes/no strings
            val = changes_to_write[ATTENDED_COLUMN]
            if isinstance(val, bool):
                changes_to_write[ATTENDED_COLUMN] = "Yes" if val else "No"
            else:
                sval = str(val).strip().lower()
                changes_to_write[ATTENDED_COLUMN] = "Yes" if sval in ["yes", "نعم", "true", "1"] else "No"

        try:
            doc_ref = db.collection(COLLECTION_NAME).document(real_doc_id)
            batch.set(doc_ref, changes_to_write, merge=True)
            update_count += 1
        except Exception as e:
            failed += 1
            st.error(f"Failed to queue update for doc {real_doc_id}: {e}")

    # Commit if any queued updates
    if update_count > 0:
        try:
            batch.commit()
            st.success(f"Successfully saved {update_count} changes to Firestore.")
        except Exception as e:
            st.error(f"Error committing updates to Firestore: {e}")
    elif failed > 0:
        st.warning("No updates were committed but some failures occurred.")

    # Clear caches so UI reloads new data
    load_data.clear()
    st.rerun()

# ------------------- Streamlit UI -------------------
st.set_page_config(page_title="Attendance Tracker", layout="wide")

# Load the data (cached)
df = load_data()

# --- If empty -> show migration UI ---
if df.empty:
    st.warning(f"The Firestore collection '{COLLECTION_NAME}' is empty or not reachable.")
    st.markdown("---")
    st.header("Step 1: Data Migration")
    st.markdown(f"Click to upload contents of **`{CSV_FILE_PATH}`** to Firestore after cleaning.")
    if st.button("Migrate Data to Firestore", type="primary"):
        migrate_csv_to_firestore()

    st.markdown("---")
    st.info("Or to debug connection, check Streamlit sidebar for Firestore connection messages.")
    st.stop()

# --- Pagination state (Next / Previous buttons) ---
total_rows = len(df)
total_pages = max(1, math.ceil(total_rows / ROWS_PER_PAGE))
if "page_number" not in st.session_state:
    st.session_state["page_number"] = 1

# Sidebar - show navigation and controls
st.sidebar.title("Navigation")
col1, col2 = st.sidebar.columns(2)
if col1.button("<-"):
    st.session_state["page_number"] = max(1, st.session_state["page_number"] - 1)
if col2.button("->"):
    st.session_state["page_number"] = min(total_pages, st.session_state["page_number"] + 1)

# Show current page and allow jump-to via a small number_input as well (optional)
st.sidebar.markdown(f"Page **{st.session_state['page_number']}** of **{total_pages}**")
st.sidebar.caption(f"Rows per page: {ROWS_PER_PAGE}")

page_number = st.session_state["page_number"]
start_idx = (page_number - 1) * ROWS_PER_PAGE
end_idx = min(start_idx + ROWS_PER_PAGE, total_rows)
page_df = df.iloc[start_idx:end_idx].copy()

st.info(f"Showing rows {start_idx+1} to {end_idx} of {total_rows} (Page {page_number} of {total_pages}).")

# Convert ATTENDED_COLUMN to boolean for the checkbox column
if ATTENDED_COLUMN in page_df.columns:
    page_df[ATTENDED_COLUMN] = page_df[ATTENDED_COLUMN].astype(str).str.lower().isin(["yes", "نعم", "true", "1"])

st.header("Attendance Records")

# Columns to hide in the editor (if present)
columns_to_hide = ["ID", "Whatsapp", "Note", "District", "_doc_id"]

# Build column_config for st.data_editor
column_configuration = {}
# Checkbox for Attended
if ATTENDED_COLUMN in page_df.columns:
    column_configuration[ATTENDED_COLUMN] = st.column_config.CheckboxColumn(
        ATTENDED_COLUMN, help="Check if the person attended.", default=False
    )

# Name column
if NAME_COLUMN in page_df.columns:
    column_configuration[NAME_COLUMN] = st.column_config.TextColumn(NAME_COLUMN)

# If Amount exists, show number column
if "Amount" in page_df.columns:
    column_configuration["Amount"] = st.column_config.NumberColumn("Amount", format="%.2f")
if "ID" in page_df.columns:
    column_configuration["ID"] = st.column_config.NumberColumn("ID")

# Add text column config for any remaining visible columns
for col in page_df.columns:
    if col in column_configuration or col in columns_to_hide:
        continue
    column_configuration[col] = st.column_config.TextColumn(col)

# Display the page slice with index visible so we can map edited_rows -> doc ids
editable_df = page_df.drop(columns=[c for c in columns_to_hide if c in page_df.columns], errors="ignore")

edited_df = st.data_editor(
    editable_df,
    column_config=column_configuration,
    hide_index=False,
    key="data_editor_key",
    num_rows="fixed",
    use_container_width=True,

)

st.markdown("---")
st.button("Save All Changes to Firestore", on_click=update_firestore_record, type="primary")

st.sidebar.divider()
# Manual backup download
csv_data = df.reset_index().to_csv(index=False).encode("utf-8-sig")
st.sidebar.download_button(
    label="Download Full Firestore Data CSV",
    data=csv_data,
    file_name="firestore_backup.csv",
    mime="text/csv",
)

st.sidebar.markdown("---")
st.sidebar.caption("Tip: Use Next / Previous to navigate pages. After editing rows on the page, click Save All Changes to push them to Firestore.")
