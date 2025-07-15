# app.py - Updated for Centralized Approach
import streamlit as st
import pandas as pd
import time
st.set_page_config(layout="wide", page_title="e-MCM App - GST Audit 1")

# --- Custom Module Imports ---
from config import MASTER_DRIVE_FOLDER_ID, CENTRALIZED_DAR_UPLOAD_FOLDER_ID, MASTER_DAR_DATABASE_SHEET_ID, MCM_INFO_SHEET_ID
from css_styles import load_custom_css
from google_utils import get_google_services, initialize_drive_structure
from ui_login import login_page
from ui_pco import pco_dashboard
from ui_audit_group import audit_group_dashboard

# --- Load CSS ---
load_custom_css()

# --- Session State Initialization ---
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'username' not in st.session_state: st.session_state.username = ""
if 'role' not in st.session_state: st.session_state.role = ""
if 'audit_group_no' not in st.session_state: st.session_state.audit_group_no = None
if 'ag_current_extracted_data' not in st.session_state: st.session_state.ag_current_extracted_data = []
if 'ag_pdf_drive_url' not in st.session_state: st.session_state.ag_pdf_drive_url = None
if 'ag_validation_errors' not in st.session_state: st.session_state.ag_validation_errors = []
if 'ag_editor_data' not in st.session_state: st.session_state.ag_editor_data = pd.DataFrame()
if 'ag_current_mcm_key' not in st.session_state: st.session_state.ag_current_mcm_key = None
if 'ag_current_uploaded_file_name' not in st.session_state: st.session_state.ag_current_uploaded_file_name = None

# For centralized Drive structure - using predefined IDs
if 'master_drive_folder_id' not in st.session_state: st.session_state.master_drive_folder_id = MASTER_DRIVE_FOLDER_ID
if 'centralized_dar_folder_id' not in st.session_state: st.session_state.centralized_dar_folder_id = CENTRALIZED_DAR_UPLOAD_FOLDER_ID
if 'master_dar_database_id' not in st.session_state: st.session_state.master_dar_database_id = MASTER_DAR_DATABASE_SHEET_ID
if 'mcm_info_sheet_id' not in st.session_state: st.session_state.mcm_info_sheet_id = MCM_INFO_SHEET_ID
if 'drive_structure_initialized' not in st.session_state: st.session_state.drive_structure_initialized = False

# --- Main App Logic ---
if not st.session_state.logged_in:
    login_page()
else:
    # Initialize Google Services if not already done
    if 'drive_service' not in st.session_state or 'sheets_service' not in st.session_state or \
            st.session_state.drive_service is None or st.session_state.sheets_service is None:
        with st.spinner("Initializing Google Services..."):
            st.session_state.drive_service, st.session_state.sheets_service = get_google_services()
            if st.session_state.drive_service and st.session_state.sheets_service:
                st.success("Google Services Initialized.")
                st.session_state.drive_structure_initialized = False  # Trigger verification
                st.rerun()
            # Error messages are handled by get_google_services()

    # Proceed only if Google services are available
    if st.session_state.drive_service and st.session_state.sheets_service:
        # Verify access to pre-created resources instead of creating them
        if not st.session_state.get('drive_structure_initialized'):
            with st.spinner("Verifying access to centralized Google Drive and Sheets resources..."):
                if initialize_drive_structure(st.session_state.drive_service):
                    st.session_state.drive_structure_initialized = True
                    st.success("✅ Access verified to centralized resources:")
                    st.info(f"📁 DAR Upload Folder: `{CENTRALIZED_DAR_UPLOAD_FOLDER_ID}`")
                    st.info(f"📊 Master Database: `{MASTER_DAR_DATABASE_SHEET_ID}`")
                    st.info(f"📋 MCM Info Sheet: `{MCM_INFO_SHEET_ID}`")
                    time.sleep(2)  # Brief pause to show success messages
                    st.rerun()
                else:
                    st.error("Failed to verify access to required Google Drive folders and Sheets. Please check service account permissions.")
                    if st.button("Logout", key="fail_logout_access_verification"):
                        st.session_state.logged_in = False; st.rerun()
                    st.stop()

        # If verification successful, route to the appropriate dashboard
        if st.session_state.get('drive_structure_initialized'):
            if st.session_state.role == "PCO":
                pco_dashboard(st.session_state.drive_service, st.session_state.sheets_service)
            elif st.session_state.role == "AuditGroup":
                audit_group_dashboard(st.session_state.drive_service, st.session_state.sheets_service)
            else:
                st.error("Unknown user role. Please login again.")
                st.session_state.logged_in = False
                st.rerun()

    elif st.session_state.logged_in:  # Logged in but services failed to initialize
        st.warning("Google services are not available. Please check configuration and network. Try logging out and back in.")
        if st.button("Logout", key="main_logout_gerror_centralized"):
            st.session_state.logged_in = False; st.rerun()

# Check for GEMINI_API_KEY
if "GEMINI_API_KEY" not in st.secrets:
    st.error("CRITICAL: 'GEMINI_API_KEY' not found in Streamlit Secrets. AI features will fail.")

# Display centralized approach info in sidebar when logged in
if st.session_state.logged_in and st.session_state.get('drive_structure_initialized'):
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 🏛️ Centralized Storage")
        st.caption("All DARs → Single Folder")
        st.caption("All Data → Master Database")
        st.markdown("---")
