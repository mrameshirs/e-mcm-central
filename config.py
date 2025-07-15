
# config.py - Updated for Centralized Approach
import streamlit as st

# --- Google API Configuration ---
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

# --- Centralized Google Drive Configuration ---
# Pre-created folder and file IDs - NO FILE CREATION
MASTER_DRIVE_FOLDER_ID = "1g1dgq5Ci_tPaqq1q2XuI7hMjiuQxDjFc"  # e-MCM App Files
CENTRALIZED_DAR_UPLOAD_FOLDER_ID = "1wptb8HtZAeFFBOJPSAJJEDvTTsQiwN2c"  # All DAR uploads
MASTER_DAR_DATABASE_SHEET_ID = "1zpkKj5hmprxpXxHuj_68hOVdBg24IwF6tFIizGU-wec"  # Master DAR database
MCM_INFO_SHEET_ID = "1rXCyGdRf8fNBr-bI9dWrfrW35UUdcM9MzXpRMTLFoW4"  # mcm_info

# Legacy config names (kept for backward compatibility in some places)
MASTER_DRIVE_FOLDER_NAME = "e-MCM App Files"  # For display purposes only
MCM_PERIODS_FILENAME_ON_DRIVE = "mcm_info"  # For display purposes only

# --- User Credentials ---
USER_CREDENTIALS = {
    "planning_officer": "pco_password",
    **{f"audit_group{i}": f"ag{i}_audit" for i in range(1, 31)}
}
USER_ROLES = {
    "planning_officer": "PCO",
    **{f"audit_group{i}": "AuditGroup" for i in range(1, 31)}
}
AUDIT_GROUP_NUMBERS = {
    f"audit_group{i}": i for i in range(1, 31)
}