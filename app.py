import streamlit as st
import pandas as pd
from google.cloud import firestore
import math
import time

# --- Configuration ---
COLLECTION_NAME = 'attendance' 
ROWS_PER_PAGE = 50
CSV_FILE_PATH = 'God_is_among_us_edited.csv'

ATTENDED_COLUMN = 'Attended'
NAME_COLUMN = 'الاسم ' 

# --- Firestore Connection Setup ---
@st.cache_resource
def get_firestore_db():
    """Initializes and returns the Firestore client."""
    
    if 'firestore' in st.secrets:
        db = firestore.Client.from_service_account_info(st.secrets['firestore'])
        st.sidebar.success("Connected to Firestore via Streamlit Secrets.")
    else:
        try:
            db = firestore.Client.from_service_account_json("firestore-key.json")
            st.sidebar.success("Connected to Firestore via local firestore-key.json.")
        except Exception as e:
            st.sidebar.error("Could not connect to Firestore. Ensure 'firestore-key.json' is present or st.secrets is configured.")
            st.stop()
            
    return db

db = get_firestore_db()

# --- CSV CLEANING FUNCTION ---
def clean_csv(df):
    """Removes rows that are completely empty."""
    df = df.dropna(how="all")
    return df

# --- DATA MIGRATION FUNCTION ---
def migrate_csv_to_firestore():
    """Reads CSV, cleans it, and uploads to Firestore."""
    st.info(f"Starting migration from {CSV_FILE_PATH} to collection: {COLLECTION_NAME}...")
    
    try:
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8')
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        return

    df = clean_csv(df)

    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])
    
    data = df.to_dict('records')
    total_records = len(data)
    
    collection_ref = db.collection(COLLECTION_NAME)
    batch = db.batch()
    batch_counter = 0
    record_counter = 0
    status_text = st.empty()
    
    for record in data:
        cleaned_record = {str(k).strip(): v for k, v in record.items() if pd.notna(v)}
        doc_ref = collection_ref.document()  # auto-generated ID
        batch.set(doc_ref, cleaned_record)
        
        batch_counter += 1
        record_counter += 1
        
        if batch_counter >= 499:
            batch.commit()
            status_text.progress(record_counter / total_records, text=f"Migrated {record_counter}/{total_records} records")
            batch = db.batch()
            batch_counter = 0
            time.sleep(0.1)

    if batch_counter > 0:
        batch.commit()

    st.success(f"Migration complete! {record_counter} records uploaded to Firestore.")
    load_data.clear()
    st.experimental_rerun()

# --- LOAD DATA FUNCTION ---
@st.cache_data(ttl=60)
def load_data():
    docs_list = []
    docs = db.collection(COLLECTION_NAME).stream()
    
    for doc in docs:
        record = doc.to_dict()
        record['_doc_id'] = str(doc.id)  # Ensure string ID
        docs_list.append(record)
        
    if not docs_list:
        return pd.DataFrame()
    
    df = pd.DataFrame(docs_list)
    df = df.set_index('_doc_id', drop=True)
    return df

# --- UPDATE FIRESTORE RECORD FUNCTION ---
def update_firestore_record():
    if 'data_editor_key' not in st.session_state:
        st.info("No changes to save.")
        return

    edited_rows = st.session_state.data_editor_key.get('edited_rows', {})

    if not edited_rows:
        st.info("No changes detected.")
        return

    batch = db.batch()
    update_count = 0

    for doc_id, changes in edited_rows.items():
        doc_ref = db.collection(COLLECTION_NAME).document(str(doc_id))

        print(changes)
        if ATTENDED_COLUMN in changes:
            changes[ATTENDED_COLUMN] = "Yes" if changes[ATTENDED_COLUMN] else "No"

        # Use set with merge=True to avoid NotFound error
        batch.set(doc_ref, changes, merge=True)
        update_count += 1

    if update_count > 0:
        batch.commit()
        st.toast(f"Successfully saved {update_count} changes to Firestore!")

    load_data.clear()
    st.session_state['rerun'] = not st.session_state.get('rerun', False)


# --- STREAMLIT UI ---
st.set_page_config(page_title="Attendance Tracker", layout="wide")
st.title("Attendance Tracker (Firebase/Firestore Backend)")

df = load_data()

if df.empty:
    st.warning(f"The Firestore collection '{COLLECTION_NAME}' is empty.")
    st.markdown("---")
    st.header("Step 1: Data Migration")
    st.markdown(f"Click the button below to upload the contents of **`{CSV_FILE_PATH}`** to Firestore after cleaning.")
    
    if st.button("Migrate Data to Firestore", type="primary"):
        migrate_csv_to_firestore()
    
else:
    total_rows = len(df)
    total_pages = math.ceil(total_rows / ROWS_PER_PAGE)
    
    st.sidebar.title("Navigation")
    page_number = st.sidebar.number_input("Page", min_value=1, max_value=total_pages, step=1, value=1)
    
    start_idx = (page_number - 1) * ROWS_PER_PAGE
    end_idx = min(start_idx + ROWS_PER_PAGE, total_rows)
    page_df = df.iloc[start_idx:end_idx].copy()
    
    st.info(f"Showing rows {start_idx + 1} to {end_idx} of {total_rows}.")
    
    # Convert ATTENDED_COLUMN to boolean
    page_df[ATTENDED_COLUMN] = page_df[ATTENDED_COLUMN].astype(str).str.lower().isin(['yes', 'نعم', 'true'])
    
    st.markdown("---")
    st.header("Attendance Records")
    
    columns_to_hide = ['ID', 'Whatsapp', 'Note', 'District'] 
    
    column_configuration = {
        ATTENDED_COLUMN: st.column_config.CheckboxColumn(
            "Attended?",
            help="Check if the person attended.",
            default=False,
        ),
        NAME_COLUMN: st.column_config.TextColumn("Name"),
        'Amount': st.column_config.NumberColumn("Amount", format='%.2f'),
    }
    
    for col in page_df.columns:
        if col not in column_configuration and col not in columns_to_hide:
            column_configuration[col] = st.column_config.TextColumn(col)
            
    edited_df = st.data_editor(
        page_df.drop(columns=[c for c in columns_to_hide if c in page_df.columns], errors='ignore'),
        column_config=column_configuration,
        hide_index=False,
        key="data_editor_key", 
        num_rows="fixed",
        use_container_width=True
    )

    st.markdown("---")
    st.button("Save All Changes to Firestore", on_click=update_firestore_record, type="primary")

    st.sidebar.divider()
    st.sidebar.caption("Manual Backup:")
    
    csv_data = df.to_csv(index=False).encode('utf-8-sig')
    st.sidebar.download_button(
        label="Download Full Firestore Data CSV",
        data=csv_data,
        file_name='firestore_backup.csv',
        mime='text/csv',
    )
