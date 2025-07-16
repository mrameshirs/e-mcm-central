# ui_audit_group.py - Updated for Centralized Approach
import streamlit as st
import pandas as pd
import datetime
import math
from io import BytesIO
import time

from google_utils import (
    load_mcm_periods, upload_to_drive, append_to_spreadsheet,
    read_from_spreadsheet, delete_spreadsheet_rows
)
from dar_processor import preprocess_pdf_text
from gemini_utils import get_structured_data_with_gemini
from validation_utils import validate_data_for_sheet, VALID_CATEGORIES, VALID_PARA_STATUSES
from config import USER_CREDENTIALS, AUDIT_GROUP_NUMBERS, MASTER_DAR_DATABASE_SHEET_ID
from models import ParsedDARReport

from streamlit_option_menu import option_menu

SHEET_DATA_COLUMNS_ORDER = [
    "audit_group_number", "audit_circle_number", "gstin", "trade_name", "category",
    "total_amount_detected_overall_rs", "total_amount_recovered_overall_rs",
    "audit_para_number", "audit_para_heading",
    "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs", "status_of_para",
]

# --- Caching helper for MCM Periods ---
def get_cached_mcm_periods_ag(sheets_service, ttl_seconds=120):
    cache_key_data = 'ag_ui_cached_mcm_periods_data'
    cache_key_ts = 'ag_ui_cached_mcm_periods_timestamp'
    current_time = time.time()
    if (cache_key_data in st.session_state and
            cache_key_ts in st.session_state and
            (current_time - st.session_state[cache_key_ts] < ttl_seconds)):
        return st.session_state[cache_key_data]
    periods = load_mcm_periods(sheets_service)
    st.session_state[cache_key_data] = periods
    st.session_state[cache_key_ts] = current_time
    return periods

# Column names for display in editor
DISPLAY_COLUMN_ORDER_EDITOR = [
    "audit_group_number", "audit_circle_number", "gstin", "trade_name", "category",
    "total_amount_detected_overall_rs", "total_amount_recovered_overall_rs",
    "audit_para_number", "audit_para_heading",
    "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs", "status_of_para"
]

def test_gemini_api():
    """Test function to verify Gemini API is working - Free Tier Friendly"""
    st.markdown("### 🔍 Test Gemini API Connection (Free Tier)")
    
    # Get API key from secrets
    api_key = st.secrets.get("GEMINI_API_KEY", "")
    
    if not api_key:
        st.error("❌ GEMINI_API_KEY not found in Streamlit secrets")
        st.info("💡 To get a free API key:")
        st.markdown("1. Go to [Google AI Studio](https://makersuite.google.com/app/apikey)")
        st.markdown("2. Click 'Create API Key'")
        st.markdown("3. Add it to your Streamlit secrets as `GEMINI_API_KEY`")
        return False
    
    if api_key == "YOUR_API_KEY_HERE":
        st.error("❌ GEMINI_API_KEY is still set to placeholder value")
        return False
    
    # Test basic API connection
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        
        # Use the free tier model
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Simple test prompt
        test_prompt = "Please respond with exactly: 'API_TEST_SUCCESS'"
        
        with st.spinner("Testing Gemini API connection..."):
            response = model.generate_content(
                test_prompt,
                generation_config=genai.types.GenerationConfig(
                    candidate_count=1,
                    temperature=0.1,
                    max_output_tokens=50,
                ),
                safety_settings=[
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
                ]
            )
        
        if response and response.text:
            response_text = response.text.strip()
            if "API_TEST_SUCCESS" in response_text:
                st.success("✅ Gemini API is working correctly!")
                st.info("🎉 Free tier access confirmed - you can process DAR files now!")
                return True
            else:
                st.success("🟡 Gemini API is responding, but format is different:")
                st.code(response_text[:200])
                st.info("This is normal - the API is working!")
                return True
        else:
            st.error("❌ Gemini API returned empty response")
            return False
            
    except Exception as e:
        error_msg = str(e)
        st.error(f"❌ Gemini API Error: {error_msg}")
        
        # Specific free tier guidance
        if "quota" in error_msg.lower() or "rate" in error_msg.lower():
            st.warning("⚠️ Free tier rate limit reached. Please wait a few minutes and try again.")
            st.info("💡 Free tier limits:")
            st.markdown("- 15 requests per minute")
            st.markdown("- 1,500 requests per day")
            st.markdown("- 1 million tokens per minute")
        elif "billing" in error_msg.lower():
            st.info("💡 This error is common but doesn't mean you need billing!")
            st.markdown("Try these steps:")
            st.markdown("1. Make sure you're using a Google account")
            st.markdown("2. Generate a new API key from Google AI Studio")
            st.markdown("3. Wait a few minutes and try again")
        elif "API_KEY" in error_msg.upper() or "auth" in error_msg.lower():
            st.info("💡 API key issue - try these steps:")
            st.markdown("1. Generate a new API key from [Google AI Studio](https://makersuite.google.com/app/apikey)")
            st.markdown("2. Make sure you're logged into the correct Google account")
            st.markdown("3. Update your Streamlit secrets with the new key")
        else:
            st.info("💡 General troubleshooting:")
            st.markdown("1. Check your internet connection")
            st.markdown("2. Try again in a few minutes")
            st.markdown("3. Generate a new API key if the problem persists")
        
        return False

def calculate_audit_circle(audit_group_number_val):
    try:
        agn = int(audit_group_number_val)
        if 1 <= agn <= 30:
            return math.ceil(agn / 3.0)
        return None
    except (ValueError, TypeError, AttributeError):
        return None

def audit_group_dashboard(drive_service, sheets_service):
    st.markdown(f"<div class='sub-header'>Audit Group {st.session_state.audit_group_no} Dashboard</div>",
                unsafe_allow_html=True)
    
    # Info about centralized approach
    st.info("📁 All DARs are uploaded to the centralized folder and stored in the Master DAR Database.")
    
    mcm_periods_all = get_cached_mcm_periods_ag(sheets_service)
    active_periods = {k: v for k, v in mcm_periods_all.items() if v.get("active")}

    YOUR_GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "YOUR_API_KEY_HERE_FALLBACK")

    default_ag_states = {
        'ag_current_mcm_key': None,
        'ag_current_uploaded_file_obj': None,
        'ag_current_uploaded_file_name': None,
        'ag_editor_data': pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR),
        'ag_pdf_drive_url': None,
        'ag_validation_errors': [],
        'ag_uploader_key_suffix': 0,
        'ag_row_to_delete_details': None,
        'ag_show_delete_confirm': False,
        'ag_deletable_map': {}
    }
    for key, value in default_ag_states.items():
        if key not in st.session_state:
            st.session_state[key] = value

    with st.sidebar:
        try: st.image("logo.png", width=80)
        except Exception: st.sidebar.markdown("*(Logo)*")
        st.markdown(f"**User:** {st.session_state.username}<br>**Group No:** {st.session_state.audit_group_no}", unsafe_allow_html=True)
        
        # Add Gemini API test
        st.markdown("---")
        st.markdown("#### 🔧 Debug Tools")
        if st.button("Test Gemini API", key="test_gemini_api_btn"):
            test_gemini_api()
        
        if st.button("Logout", key="ag_logout_centralized", use_container_width=True):
            keys_to_clear = list(default_ag_states.keys()) + ['drive_structure_initialized', 'ag_ui_cached_mcm_periods_data', 'ag_ui_cached_mcm_periods_timestamp']
            for ktd in keys_to_clear:
                if ktd in st.session_state: del st.session_state[ktd]
            st.session_state.logged_in = False; st.session_state.username = ""; st.session_state.role = ""; st.session_state.audit_group_no = None
            st.rerun()
        st.markdown("---")

    selected_tab = option_menu(
        menu_title=None, options=["Upload DAR for MCM", "View My Uploaded DARs", "Delete My DAR Entries"],
        icons=["cloud-upload-fill", "eye-fill", "trash2-fill"], menu_icon="person-workspace", default_index=0, orientation="horizontal",
        styles={
            "container": {"padding": "5px !important", "background-color": "#e9ecef"}, "icon": {"color": "#28a745", "font-size": "20px"},
            "nav-link": {"font-size": "16px", "text-align": "center", "margin": "0px", "--hover-color": "#d4edda"},
            "nav-link-selected": {"background-color": "#28a745", "color": "white"},
        })
    st.markdown("<div class='card'>", unsafe_allow_html=True)

    # ========================== UPLOAD DAR FOR MCM TAB ==========================
    if selected_tab == "Upload DAR for MCM":
        st.markdown("<h3>Upload DAR PDF for MCM Period</h3>", unsafe_allow_html=True)
        if not active_periods:
            st.warning("No active MCM periods. Contact Planning Officer.")
        else:
            period_options_disp_map = {k: f"{v.get('month_name')} {v.get('year')}" for k, v in sorted(active_periods.items(), key=lambda x: x[0], reverse=True) if v.get('month_name') and v.get('year')}
            period_select_map_rev = {v: k for k, v in period_options_disp_map.items()}
            current_mcm_display_val = period_options_disp_map.get(st.session_state.ag_current_mcm_key)
            
            selected_period_str = st.selectbox(
                "Select Active MCM Period", options=list(period_select_map_rev.keys()),
                index=list(period_select_map_rev.keys()).index(current_mcm_display_val) if current_mcm_display_val and current_mcm_display_val in period_select_map_rev else 0 if period_select_map_rev else None,
                key=f"ag_mcm_sel_centralized_{st.session_state.ag_uploader_key_suffix}"
            )

            if selected_period_str:
                new_mcm_key = period_select_map_rev[selected_period_str]
                mcm_info_current = active_periods[new_mcm_key]

                if st.session_state.ag_current_mcm_key != new_mcm_key:
                    st.session_state.ag_current_mcm_key = new_mcm_key
                    st.session_state.ag_current_uploaded_file_obj = None; st.session_state.ag_current_uploaded_file_name = None
                    st.session_state.ag_editor_data = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR); st.session_state.ag_pdf_drive_url = None
                    st.session_state.ag_validation_errors = []; st.session_state.ag_uploader_key_suffix += 1
                    st.rerun()

                st.info(f"Uploading for: {mcm_info_current['month_name']} {mcm_info_current['year']} → Centralized Storage")
                uploaded_file = st.file_uploader("Choose DAR PDF", type="pdf", key=f"ag_uploader_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_uploader_key_suffix}")

                if uploaded_file:
                    if st.session_state.ag_current_uploaded_file_name != uploaded_file.name or st.session_state.ag_current_uploaded_file_obj is None:
                        st.session_state.ag_current_uploaded_file_obj = uploaded_file; st.session_state.ag_current_uploaded_file_name = uploaded_file.name
                        st.session_state.ag_editor_data = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR); st.session_state.ag_pdf_drive_url = None
                        st.session_state.ag_validation_errors = []

                extract_button_key = f"extract_data_btn_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_current_uploaded_file_name or 'no_file_yet'}"
                if st.session_state.ag_current_uploaded_file_obj and st.button("Extract Data from PDF", key=extract_button_key, use_container_width=True):
                    with st.spinner(f"Processing '{st.session_state.ag_current_uploaded_file_name}'... This might take a moment."):
                        pdf_bytes = st.session_state.ag_current_uploaded_file_obj.getvalue()
                        st.session_state.ag_pdf_drive_url = None 
                        st.session_state.ag_validation_errors = []

                        # Use centralized upload (no folder_id parameter needed)
                        dar_filename_on_drive = f"AG{st.session_state.audit_group_no}_{st.session_state.ag_current_uploaded_file_name}"
                        pdf_drive_id, pdf_drive_url_temp = upload_to_drive(drive_service, BytesIO(pdf_bytes), dar_filename_on_drive)
                        temp_list_for_df = []
                        
                        if not pdf_drive_id:
                            st.error("Failed to upload PDF to Drive. Cannot proceed with extraction.")
                            base_row_manual = {col: None for col in DISPLAY_COLUMN_ORDER_EDITOR}
                            base_row_manual.update({"audit_group_number": st.session_state.audit_group_no, "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no), "audit_para_heading": "Manual Entry - PDF Upload Failed"})
                            temp_list_for_df.append(base_row_manual)
                        else:
                            st.session_state.ag_pdf_drive_url = pdf_drive_url_temp
                            st.success(f"DAR PDF uploaded to Centralized Drive: [Link]({st.session_state.ag_pdf_drive_url})")
                            
                            # Add debugging info
                            st.info("Starting PDF text extraction...")
                            preprocessed_text = preprocess_pdf_text(BytesIO(pdf_bytes))

                            if preprocessed_text.startswith("Error"):
                                st.error(f"PDF Preprocessing Error: {preprocessed_text}")
                                base_row_manual = {col: None for col in DISPLAY_COLUMN_ORDER_EDITOR}
                                base_row_manual.update({"audit_group_number": st.session_state.audit_group_no, "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no), "audit_para_heading": "Manual Entry - PDF Processing Error"})
                                temp_list_for_df.append(base_row_manual)
                            else:
                                st.info(f"PDF text extracted successfully. Length: {len(preprocessed_text)} characters")
                                st.info("Starting AI analysis with Gemini...")
                                
                                # Add error handling for Gemini API
                                try:
                                    parsed_data: ParsedDARReport = get_structured_data_with_gemini(YOUR_GEMINI_API_KEY, preprocessed_text)
                                    
                                    if parsed_data.parsing_errors: 
                                        st.warning(f"AI Parsing Issues: {parsed_data.parsing_errors}")
                                        # Show first 500 chars of extracted text for debugging
                                        with st.expander("Show extracted text preview (for debugging)"):
                                            st.text(preprocessed_text[:500] + "..." if len(preprocessed_text) > 500 else preprocessed_text)
                                
                                    header_dict = parsed_data.header.model_dump() if parsed_data.header else {}
                                    st.info(f"AI extracted header info: {header_dict}")
                                    
                                    base_info = {
                                        "audit_group_number": st.session_state.audit_group_no,
                                        "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no),
                                        "gstin": header_dict.get("gstin"), 
                                        "trade_name": header_dict.get("trade_name"), 
                                        "category": header_dict.get("category"),
                                        "total_amount_detected_overall_rs": header_dict.get("total_amount_detected_overall_rs"),
                                        "total_amount_recovered_overall_rs": header_dict.get("total_amount_recovered_overall_rs"),
                                    }
                                    
                                    if parsed_data.audit_paras:
                                        st.info(f"AI found {len(parsed_data.audit_paras)} audit paras")
                                        for para_obj in parsed_data.audit_paras:
                                            para_dict = para_obj.model_dump()
                                            row = base_info.copy()
                                            row.update({k: para_dict.get(k) for k in ["audit_para_number", "audit_para_heading", "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs", "status_of_para"]})
                                            temp_list_for_df.append(row)
                                    elif base_info.get("trade_name"):
                                        st.info("AI extracted header data but no specific paras found")
                                        row = base_info.copy()
                                        row.update({"audit_para_number": None, "audit_para_heading": "N/A - Header Info Only (Add Paras Manually)", "status_of_para": None})
                                        temp_list_for_df.append(row)
                                    else:
                                        st.error("AI failed to extract key header information")
                                        row = base_info.copy()
                                        row.update({"audit_para_heading": "Manual Entry Required - AI Extraction Failed", "status_of_para": None})
                                        temp_list_for_df.append(row)
                                        
                                except Exception as e_gemini:
                                    st.error(f"Gemini API Error: {str(e_gemini)}")
                                    st.error("Please check your Gemini API key and try again.")
                                    # Show the extracted text for manual review
                                    with st.expander("Show extracted text for manual review"):
                                        st.text_area("Extracted Text", preprocessed_text, height=300)
                                    
                                    # Create fallback entry
                                    base_row_manual = {col: None for col in DISPLAY_COLUMN_ORDER_EDITOR}
                                    base_row_manual.update({
                                        "audit_group_number": st.session_state.audit_group_no, 
                                        "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no), 
                                        "audit_para_heading": "Manual Entry Required - Gemini API Error"
                                    })
                                    temp_list_for_df.append(base_row_manual)
                        
                        if not temp_list_for_df: 
                             base_row_manual = {col: None for col in DISPLAY_COLUMN_ORDER_EDITOR}
                             base_row_manual.update({"audit_group_number": st.session_state.audit_group_no, "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no), "audit_para_heading": "Manual Entry - Extraction Issue"})
                             temp_list_for_df.append(base_row_manual)
                        
                        df_extracted = pd.DataFrame(temp_list_for_df)
                        for col in DISPLAY_COLUMN_ORDER_EDITOR:
                            if col not in df_extracted.columns: df_extracted[col] = None
                        st.session_state.ag_editor_data = df_extracted[DISPLAY_COLUMN_ORDER_EDITOR]
                        st.success("Data extraction processed. Review and edit below.")
                        st.rerun()

                # --- Data Editor and Submission ---
                edited_df_local_copy = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR)
                if not st.session_state.ag_editor_data.empty:
                    st.markdown("<h4>Review and Edit Extracted Data:</h4>", unsafe_allow_html=True)
                    col_conf = {
                        "audit_group_number": st.column_config.NumberColumn(disabled=True), "audit_circle_number": st.column_config.NumberColumn(disabled=True),
                        "gstin": st.column_config.TextColumn(width="medium"), "trade_name": st.column_config.TextColumn(width="large"),
                        "category": st.column_config.SelectboxColumn(options=[None] + VALID_CATEGORIES, required=False, width="small"),
                        "total_amount_detected_overall_rs": st.column_config.NumberColumn("Total Detect (Rs)", format="%.2f", width="medium"),
                        "total_amount_recovered_overall_rs": st.column_config.NumberColumn("Total Recover (Rs)", format="%.2f", width="medium"),
                        "audit_para_number": st.column_config.NumberColumn("Para No.", format="%d", width="small", help="Integer only"),
                        "audit_para_heading": st.column_config.TextColumn("Para Heading", width="xlarge"),
                        "revenue_involved_lakhs_rs": st.column_config.NumberColumn("Rev. Involved (Lakhs)", format="%.2f", width="small"),
                        "revenue_recovered_lakhs_rs": st.column_config.NumberColumn("Rev. Recovered (Lakhs)", format="%.2f", width="small"),
                        "status_of_para": st.column_config.SelectboxColumn("Para Status", options=[None] + VALID_PARA_STATUSES, required=False, width="medium")}
                    final_editor_col_conf = {k: v for k, v in col_conf.items() if k in DISPLAY_COLUMN_ORDER_EDITOR}
                    
                    editor_key = f"data_editor_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_current_uploaded_file_name or 'no_file_active'}"
                    
                    edited_df_local_copy = pd.DataFrame(st.data_editor(
                        st.session_state.ag_editor_data.copy(),
                        column_config=final_editor_col_conf, num_rows="dynamic",
                        key=editor_key, use_container_width=True, hide_index=True, 
                        height=min(len(st.session_state.ag_editor_data) * 45 + 70, 450) if not st.session_state.ag_editor_data.empty else 200
                    ))

                submit_button_key = f"submit_btn_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_current_uploaded_file_name or 'no_file_active'}"
                can_submit = not edited_df_local_copy.empty if not st.session_state.ag_editor_data.empty else False
                if st.button("Validate and Submit to Master Database", key=submit_button_key, use_container_width=True, disabled=not can_submit):
                    df_from_editor = edited_df_local_copy.copy()

                    # Drop completely empty rows
                    df_to_submit = df_from_editor.dropna(how='all').reset_index(drop=True)

                    if df_to_submit.empty and not df_from_editor.empty:
                        st.error("Submission failed: Only empty rows were found. Please fill in the details.")
                    else:
                        # Check for missing data in essential columns
                        required_cols = ['gstin', 'trade_name', 'audit_para_heading']
                        missing_required = df_to_submit[required_cols].isnull().any(axis=1)

                        if missing_required.any():
                            st.error("Submission failed: At least one row is missing required information (e.g., GSTIN, Trade Name, or Para Heading). Please complete all fields.")
                        else:
                            df_to_submit["audit_group_number"] = st.session_state.audit_group_no
                            df_to_submit["audit_circle_number"] = calculate_audit_circle(st.session_state.audit_group_no)

                            num_cols_to_convert = ["total_amount_detected_overall_rs", "total_amount_recovered_overall_rs", "audit_para_number", "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs"]
                            for nc in num_cols_to_convert:
                                if nc in df_to_submit.columns: df_to_submit[nc] = pd.to_numeric(df_to_submit[nc], errors='coerce')
                            
                            st.session_state.ag_validation_errors = validate_data_for_sheet(df_to_submit)
   
                            if not st.session_state.ag_validation_errors:
                                if not st.session_state.ag_pdf_drive_url: 
                                    st.error("PDF Drive URL missing. This indicates the initial PDF upload with extraction failed. Please re-extract data."); st.stop()

                                with st.spinner("Submitting to Master DAR Database..."):
                                    rows_for_sheet = []; ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    mcm_period_str = f"{mcm_info_current['month_name']} {mcm_info_current['year']}"
                                    
                                    final_df_for_sheet_upload = df_to_submit.copy()
                                    for sheet_col_name in SHEET_DATA_COLUMNS_ORDER:
                                        if sheet_col_name not in final_df_for_sheet_upload.columns:
                                            final_df_for_sheet_upload[sheet_col_name] = None
                                    
                                    final_df_for_sheet_upload["audit_group_number"] = st.session_state.audit_group_no
                                    final_df_for_sheet_upload["audit_circle_number"] = calculate_audit_circle(st.session_state.audit_group_no)
                                    
                                    for _, r_data_submit in final_df_for_sheet_upload.iterrows():
                                        # Updated to include MCM Period in the row
                                        sheet_row = [r_data_submit.get(col) for col in SHEET_DATA_COLUMNS_ORDER] + [st.session_state.ag_pdf_drive_url, ts, mcm_period_str]
                                        rows_for_sheet.append(sheet_row)
                                    
                                    if rows_for_sheet:
                                        # Use centralized append function (no spreadsheet_id parameter needed)
                                        if append_to_spreadsheet(sheets_service, rows_for_sheet):
                                            st.success("Data submitted successfully to Master DAR Database!"); st.balloons(); time.sleep(1)
                                            st.session_state.ag_current_uploaded_file_obj = None; st.session_state.ag_current_uploaded_file_name = None
                                            st.session_state.ag_editor_data = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR); st.session_state.ag_pdf_drive_url = None
                                            st.session_state.ag_validation_errors = []; st.session_state.ag_uploader_key_suffix += 1
                                            st.rerun()
                                        else: st.error("Failed to append to Master DAR Database.")
                                    else: st.error("No data rows to submit.")
                            else:
                                st.error("Validation Failed! Correct errors.");
                                if st.session_state.ag_validation_errors: st.subheader("⚠️ Validation Errors:"); [st.warning(f"- {err}") for err in st.session_state.ag_validation_errors]
            elif not period_select_map_rev: st.info("No MCM periods available.")

    # ========================== VIEW MY UPLOADED DARS TAB ==========================
    elif selected_tab == "View My Uploaded DARs":
        st.markdown("<h3>My Uploaded DARs</h3>", unsafe_allow_html=True)
        st.info("📊 Viewing data from the centralized Master DAR Database.")
        
        if not mcm_periods_all: 
            st.info("No MCM periods found.")
        else:
            view_period_opts_map = {k: f"{p.get('month_name')} {p.get('year')}" for k, p in sorted(mcm_periods_all.items(), key=lambda x: x[0], reverse=True) if p.get('month_name') and p.get('year')}
            if not view_period_opts_map and mcm_periods_all: 
                st.warning("Some MCM periods have incomplete data.")
            if not view_period_opts_map: 
                st.info("No valid MCM periods to view.")
            else:
                sel_view_key = st.selectbox("Select MCM Period", options=list(view_period_opts_map.keys()), format_func=lambda k: view_period_opts_map[k], key="ag_view_sel_centralized")
                if sel_view_key and sheets_service:
                    mcm_period_str = view_period_opts_map[sel_view_key]
                    
                    with st.spinner("Loading uploads from Master Database..."): 
                        df_sheet_all = read_from_spreadsheet(sheets_service)
                    
                    if df_sheet_all is not None and not df_sheet_all.empty:
                        # Filter by audit group and MCM period
                        if "Audit Group Number" in df_sheet_all.columns:
                            df_sheet_all["Audit Group Number"] = df_sheet_all["Audit Group Number"].astype(str)
                            my_uploads = df_sheet_all[df_sheet_all["Audit Group Number"] == str(st.session_state.audit_group_no)]
                            
                            # Further filter by MCM Period if column exists
                            if 'MCM Period' in my_uploads.columns:
                                my_uploads = my_uploads[my_uploads['MCM Period'] == mcm_period_str]
                            
                            if not my_uploads.empty:
                                st.markdown(f"<h4>Your Uploads for {mcm_period_str}:</h4>", unsafe_allow_html=True)
                                my_uploads_disp = my_uploads.copy()
                                if "DAR PDF URL" in my_uploads_disp.columns:
                                    my_uploads_disp['DAR PDF URL Links'] = my_uploads_disp["DAR PDF URL"].apply(lambda x: f'<a href="{x}" target="_blank">View PDF</a>' if pd.notna(x) and str(x).startswith("http") else "No Link")
                                
                                cols_to_view_final = [
                                    "Audit Circle Number", "GSTIN", "Trade Name", "Category",
                                    "Total Amount Detected (Overall Rs)", "Total Amount Recovered (Overall Rs)",
                                    "Audit Para Number", "Audit Para Heading", "Status of para",
                                    "Revenue Involved (Lakhs Rs)", "Revenue Recovered (Lakhs Rs)",
                                    "DAR PDF URL Links",
                                    "Record Created Date"
                                ]
                                existing_cols_to_display = [c for c in cols_to_view_final if c in my_uploads_disp.columns]
                                
                                if not existing_cols_to_display:
                                    st.warning("No relevant columns found to display for your uploads. Please check sheet structure.")
                                else:
                                    st.markdown(my_uploads_disp[existing_cols_to_display].to_html(escape=False, index=False), unsafe_allow_html=True)
                            else: 
                                st.info(f"No DARs by you for {mcm_period_str}.")
                        else: 
                            st.warning("Sheet missing 'Audit Group Number' column or data malformed.")
                    elif df_sheet_all is None: 
                        st.error("Error reading Master DAR Database for viewing.")
                    else: 
                        st.info(f"No data in Master DAR Database for {mcm_period_str}.")
                elif not sheets_service and sel_view_key: 
                    st.error("Google Sheets service unavailable.")

    # ========================== DELETE MY DAR ENTRIES TAB ==========================
    elif selected_tab == "Delete My DAR Entries":
        st.markdown("<h3>Delete My Uploaded DAR Entries</h3>", unsafe_allow_html=True)
        st.info("⚠️ This action is irreversible. Deletion removes entries from the Master DAR Database; the PDF in centralized storage will remain.")
        
        if not mcm_periods_all: 
            st.info("No MCM periods found.")
        else:
            del_period_opts_map = {k: f"{p.get('month_name')} {p.get('year')}" for k, p in sorted(mcm_periods_all.items(), key=lambda x: x[0], reverse=True) if p.get('month_name') and p.get('year')}
            if not del_period_opts_map and mcm_periods_all: 
                st.warning("Some MCM periods have incomplete data.")
            if not del_period_opts_map: 
                st.info("No valid MCM periods to manage entries.")
            else:
                sel_del_key = st.selectbox("Select MCM Period", options=list(del_period_opts_map.keys()), format_func=lambda k: del_period_opts_map[k], key="ag_del_sel_centralized")
                if sel_del_key and sheets_service:
                    mcm_period_str = del_period_opts_map[sel_del_key]
                    del_sheet_gid = 0
                    try: 
                        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=MASTER_DAR_DATABASE_SHEET_ID).execute()
                        del_sheet_gid = sheet_metadata.get('sheets', [{}])[0].get('properties', {}).get('sheetId', 0)
                    except Exception as e_gid: 
                        st.error(f"Could not get sheet GID: {e_gid}"); st.stop()

                    with st.spinner("Loading entries from Master Database..."): 
                        df_all_del_data = read_from_spreadsheet(sheets_service)
                        
                    if df_all_del_data is not None and not df_all_del_data.empty:
                        if 'Audit Group Number' in df_all_del_data.columns:
                            df_all_del_data['Audit Group Number'] = df_all_del_data['Audit Group Number'].astype(str)
                            my_entries_del = df_all_del_data[df_all_del_data['Audit Group Number'] == str(st.session_state.audit_group_no)].copy()
                            
                            # Further filter by MCM Period if column exists
                            if 'MCM Period' in my_entries_del.columns:
                                my_entries_del = my_entries_del[my_entries_del['MCM Period'] == mcm_period_str]
                            
                            my_entries_del['original_data_index'] = my_entries_del.index 

                            if not my_entries_del.empty:
                                st.markdown(f"<h4>Your Uploads in {mcm_period_str} (Select to delete):</h4>", unsafe_allow_html=True)
                                del_options_disp = ["--Select an entry to delete--"]; st.session_state.ag_deletable_map.clear()
                                for _, del_row in my_entries_del.iterrows():
                                    del_ident = f"TN: {str(del_row.get('Trade Name', 'N/A'))[:20]} | Para: {del_row.get('Audit Para Number', 'N/A')} | Date: {del_row.get('Record Created Date', 'N/A')}"
                                    del_options_disp.append(del_ident)
                                    st.session_state.ag_deletable_map[del_ident] = {
                                        "original_df_index": del_row['original_data_index'],
                                        "Trade Name": str(del_row.get('Trade Name')),
                                        "Audit Para Number": str(del_row.get('Audit Para Number')),
                                        "Record Created Date": str(del_row.get('Record Created Date')),
                                        "DAR PDF URL": str(del_row.get('DAR PDF URL'))
                                    }
                                
                                sel_entry_del_str = st.selectbox("Select Entry:", options=del_options_disp, key=f"del_box_centralized_{sel_del_key}")
                                if sel_entry_del_str != "--Select an entry to delete--":
                                    entry_info_to_delete = st.session_state.ag_deletable_map.get(sel_entry_del_str)
                                    if entry_info_to_delete is not None :
                                        orig_idx_to_del = entry_info_to_delete["original_df_index"]
                                        st.warning(f"Confirm Deletion: TN: **{entry_info_to_delete.get('Trade Name')}**, Para: **{entry_info_to_delete.get('Audit Para Number')}**")
                                        with st.form(key=f"del_form_centralized_{orig_idx_to_del}"):
                                            pwd = st.text_input("Password:", type="password", key=f"del_pwd_centralized_{orig_idx_to_del}")
                                            if st.form_submit_button("Yes, Delete This Entry"):
                                                if pwd == USER_CREDENTIALS.get(st.session_state.username):
                                                    if delete_spreadsheet_rows(sheets_service, del_sheet_gid, [orig_idx_to_del]): 
                                                        st.success("Entry deleted from Master Database."); time.sleep(1); st.rerun()
                                                    else: st.error("Failed to delete from Master Database.")
                                                else: st.error("Incorrect password.")
                                    else: st.error("Could not identify selected entry. Please refresh and re-select.")
                            else: st.info(f"You have no entries in {mcm_period_str} to delete.")
                        else: st.warning("Sheet missing 'Audit Group Number' column.")
                    elif df_all_del_data is None: st.error("Error reading Master Database for deletion.")
                    else: st.info(f"No data in Master Database for {mcm_period_str}.")
                elif not sheets_service and sel_del_key: st.error("Google Sheets service unavailable.")

    st.markdown("</div>", unsafe_allow_html=True)# # ui_audit_group.py - Updated for Centralized Approach
# import streamlit as st
# import pandas as pd
# import datetime
# import math
# from io import BytesIO
# import time

# from google_utils import (
#     load_mcm_periods, upload_to_drive, append_to_spreadsheet,
#     read_from_spreadsheet, delete_spreadsheet_rows
# )
# from dar_processor import preprocess_pdf_text
# from gemini_utils import get_structured_data_with_gemini
# from validation_utils import validate_data_for_sheet, VALID_CATEGORIES, VALID_PARA_STATUSES
# from config import USER_CREDENTIALS, AUDIT_GROUP_NUMBERS, MASTER_DAR_DATABASE_SHEET_ID
# from models import ParsedDARReport

# from streamlit_option_menu import option_menu

# SHEET_DATA_COLUMNS_ORDER = [
#     "audit_group_number", "audit_circle_number", "gstin", "trade_name", "category",
#     "total_amount_detected_overall_rs", "total_amount_recovered_overall_rs",
#     "audit_para_number", "audit_para_heading",
#     "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs", "status_of_para",
# ]

# # --- Caching helper for MCM Periods ---
# def get_cached_mcm_periods_ag(sheets_service, ttl_seconds=120):
#     cache_key_data = 'ag_ui_cached_mcm_periods_data'
#     cache_key_ts = 'ag_ui_cached_mcm_periods_timestamp'
#     current_time = time.time()
#     if (cache_key_data in st.session_state and
#             cache_key_ts in st.session_state and
#             (current_time - st.session_state[cache_key_ts] < ttl_seconds)):
#         return st.session_state[cache_key_data]
#     periods = load_mcm_periods(sheets_service)
#     st.session_state[cache_key_data] = periods
#     st.session_state[cache_key_ts] = current_time
#     return periods

# # Column names for display in editor
# DISPLAY_COLUMN_ORDER_EDITOR = [
#     "audit_group_number", "audit_circle_number", "gstin", "trade_name", "category",
#     "total_amount_detected_overall_rs", "total_amount_recovered_overall_rs",
#     "audit_para_number", "audit_para_heading",
#     "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs", "status_of_para"
# ]

# def calculate_audit_circle(audit_group_number_val):
#     try:
#         agn = int(audit_group_number_val)
#         if 1 <= agn <= 30:
#             return math.ceil(agn / 3.0)
#         return None
#     except (ValueError, TypeError, AttributeError):
#         return None

# def audit_group_dashboard(drive_service, sheets_service):
#     st.markdown(f"<div class='sub-header'>Audit Group {st.session_state.audit_group_no} Dashboard</div>",
#                 unsafe_allow_html=True)
    
#     # Info about centralized approach
#     st.info("📁 All DARs are uploaded to the centralized folder and stored in the Master DAR Database.")
    
#     mcm_periods_all = get_cached_mcm_periods_ag(sheets_service)
#     active_periods = {k: v for k, v in mcm_periods_all.items() if v.get("active")}

#     YOUR_GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "YOUR_API_KEY_HERE_FALLBACK")

#     default_ag_states = {
#         'ag_current_mcm_key': None,
#         'ag_current_uploaded_file_obj': None,
#         'ag_current_uploaded_file_name': None,
#         'ag_editor_data': pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR),
#         'ag_pdf_drive_url': None,
#         'ag_validation_errors': [],
#         'ag_uploader_key_suffix': 0,
#         'ag_row_to_delete_details': None,
#         'ag_show_delete_confirm': False,
#         'ag_deletable_map': {}
#     }
#     for key, value in default_ag_states.items():
#         if key not in st.session_state:
#             st.session_state[key] = value

#     with st.sidebar:
#         try: st.image("logo.png", width=80)
#         except Exception: st.sidebar.markdown("*(Logo)*")
#         st.markdown(f"**User:** {st.session_state.username}<br>**Group No:** {st.session_state.audit_group_no}", unsafe_allow_html=True)
#         if st.button("Logout", key="ag_logout_centralized", use_container_width=True):
#             keys_to_clear = list(default_ag_states.keys()) + ['drive_structure_initialized', 'ag_ui_cached_mcm_periods_data', 'ag_ui_cached_mcm_periods_timestamp']
#             for ktd in keys_to_clear:
#                 if ktd in st.session_state: del st.session_state[ktd]
#             st.session_state.logged_in = False; st.session_state.username = ""; st.session_state.role = ""; st.session_state.audit_group_no = None
#             st.rerun()
#         st.markdown("---")

#     selected_tab = option_menu(
#         menu_title=None, options=["Upload DAR for MCM", "View My Uploaded DARs", "Delete My DAR Entries"],
#         icons=["cloud-upload-fill", "eye-fill", "trash2-fill"], menu_icon="person-workspace", default_index=0, orientation="horizontal",
#         styles={
#             "container": {"padding": "5px !important", "background-color": "#e9ecef"}, "icon": {"color": "#28a745", "font-size": "20px"},
#             "nav-link": {"font-size": "16px", "text-align": "center", "margin": "0px", "--hover-color": "#d4edda"},
#             "nav-link-selected": {"background-color": "#28a745", "color": "white"},
#         })
#     st.markdown("<div class='card'>", unsafe_allow_html=True)

#     # ========================== UPLOAD DAR FOR MCM TAB ==========================
#     if selected_tab == "Upload DAR for MCM":
#         st.markdown("<h3>Upload DAR PDF for MCM Period</h3>", unsafe_allow_html=True)
#         if not active_periods:
#             st.warning("No active MCM periods. Contact Planning Officer.")
#         else:
#             period_options_disp_map = {k: f"{v.get('month_name')} {v.get('year')}" for k, v in sorted(active_periods.items(), key=lambda x: x[0], reverse=True) if v.get('month_name') and v.get('year')}
#             period_select_map_rev = {v: k for k, v in period_options_disp_map.items()}
#             current_mcm_display_val = period_options_disp_map.get(st.session_state.ag_current_mcm_key)
            
#             selected_period_str = st.selectbox(
#                 "Select Active MCM Period", options=list(period_select_map_rev.keys()),
#                 index=list(period_select_map_rev.keys()).index(current_mcm_display_val) if current_mcm_display_val and current_mcm_display_val in period_select_map_rev else 0 if period_select_map_rev else None,
#                 key=f"ag_mcm_sel_centralized_{st.session_state.ag_uploader_key_suffix}"
#             )

#             if selected_period_str:
#                 new_mcm_key = period_select_map_rev[selected_period_str]
#                 mcm_info_current = active_periods[new_mcm_key]

#                 if st.session_state.ag_current_mcm_key != new_mcm_key:
#                     st.session_state.ag_current_mcm_key = new_mcm_key
#                     st.session_state.ag_current_uploaded_file_obj = None; st.session_state.ag_current_uploaded_file_name = None
#                     st.session_state.ag_editor_data = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR); st.session_state.ag_pdf_drive_url = None
#                     st.session_state.ag_validation_errors = []; st.session_state.ag_uploader_key_suffix += 1
#                     st.rerun()

#                 st.info(f"Uploading for: {mcm_info_current['month_name']} {mcm_info_current['year']} → Centralized Storage")
#                 uploaded_file = st.file_uploader("Choose DAR PDF", type="pdf", key=f"ag_uploader_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_uploader_key_suffix}")

#                 if uploaded_file:
#                     if st.session_state.ag_current_uploaded_file_name != uploaded_file.name or st.session_state.ag_current_uploaded_file_obj is None:
#                         st.session_state.ag_current_uploaded_file_obj = uploaded_file; st.session_state.ag_current_uploaded_file_name = uploaded_file.name
#                         st.session_state.ag_editor_data = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR); st.session_state.ag_pdf_drive_url = None
#                         st.session_state.ag_validation_errors = []

#                 extract_button_key = f"extract_data_btn_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_current_uploaded_file_name or 'no_file_yet'}"
#                 if st.session_state.ag_current_uploaded_file_obj and st.button("Extract Data from PDF", key=extract_button_key, use_container_width=True):
#                     with st.spinner(f"Processing '{st.session_state.ag_current_uploaded_file_name}'... This might take a moment."):
#                         pdf_bytes = st.session_state.ag_current_uploaded_file_obj.getvalue()
#                         st.session_state.ag_pdf_drive_url = None 
#                         st.session_state.ag_validation_errors = []

#                         # Use centralized upload (no folder_id parameter needed)
#                         dar_filename_on_drive = f"AG{st.session_state.audit_group_no}_{st.session_state.ag_current_uploaded_file_name}"
#                         pdf_drive_id, pdf_drive_url_temp = upload_to_drive(drive_service, BytesIO(pdf_bytes), dar_filename_on_drive)
#                         temp_list_for_df = []
                        
#                         if not pdf_drive_id:
#                             st.error("Failed to upload PDF to Drive. Cannot proceed with extraction.")
#                             base_row_manual = {col: None for col in DISPLAY_COLUMN_ORDER_EDITOR}
#                             base_row_manual.update({"audit_group_number": st.session_state.audit_group_no, "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no), "audit_para_heading": "Manual Entry - PDF Upload Failed"})
#                             temp_list_for_df.append(base_row_manual)
#                         else:
#                             st.session_state.ag_pdf_drive_url = pdf_drive_url_temp
#                             st.success(f"DAR PDF uploaded to Centralized Drive: [Link]({st.session_state.ag_pdf_drive_url})")
#                             preprocessed_text = preprocess_pdf_text(BytesIO(pdf_bytes))

#                             if preprocessed_text.startswith("Error"):
#                                 st.error(f"PDF Preprocessing Error: {preprocessed_text}")
#                                 base_row_manual = {col: None for col in DISPLAY_COLUMN_ORDER_EDITOR}
#                                 base_row_manual.update({"audit_group_number": st.session_state.audit_group_no, "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no), "audit_para_heading": "Manual Entry - PDF Error"})
#                                 temp_list_for_df.append(base_row_manual)
#                             else:
#                                 parsed_data: ParsedDARReport = get_structured_data_with_gemini(YOUR_GEMINI_API_KEY, preprocessed_text)
#                                 if parsed_data.parsing_errors: st.warning(f"AI Parsing Issues: {parsed_data.parsing_errors}")

#                                 header_dict = parsed_data.header.model_dump() if parsed_data.header else {}
#                                 base_info = {
#                                     "audit_group_number": st.session_state.audit_group_no,
#                                     "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no),
#                                     "gstin": header_dict.get("gstin"), "trade_name": header_dict.get("trade_name"), "category": header_dict.get("category"),
#                                     "total_amount_detected_overall_rs": header_dict.get("total_amount_detected_overall_rs"),
#                                     "total_amount_recovered_overall_rs": header_dict.get("total_amount_recovered_overall_rs"),
#                                 }
#                                 if parsed_data.audit_paras:
#                                     for para_obj in parsed_data.audit_paras:
#                                         para_dict = para_obj.model_dump(); row = base_info.copy(); row.update({k: para_dict.get(k) for k in ["audit_para_number", "audit_para_heading", "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs", "status_of_para"]}); temp_list_for_df.append(row)
#                                 elif base_info.get("trade_name"):
#                                     row = base_info.copy(); row.update({"audit_para_number": None, "audit_para_heading": "N/A - Header Info Only (Add Paras Manually)", "status_of_para": None}); temp_list_for_df.append(row)
#                                 else:
#                                     st.error("AI failed key header info."); row = base_info.copy(); row.update({"audit_para_heading": "Manual Entry Required", "status_of_para": None}); temp_list_for_df.append(row)
                        
#                         if not temp_list_for_df: 
#                              base_row_manual = {col: None for col in DISPLAY_COLUMN_ORDER_EDITOR}
#                              base_row_manual.update({"audit_group_number": st.session_state.audit_group_no, "audit_circle_number": calculate_audit_circle(st.session_state.audit_group_no), "audit_para_heading": "Manual Entry - Extraction Issue"})
#                              temp_list_for_df.append(base_row_manual)
                        
#                         df_extracted = pd.DataFrame(temp_list_for_df)
#                         for col in DISPLAY_COLUMN_ORDER_EDITOR:
#                             if col not in df_extracted.columns: df_extracted[col] = None
#                         st.session_state.ag_editor_data = df_extracted[DISPLAY_COLUMN_ORDER_EDITOR]
#                         st.success("Data extraction processed. Review and edit below.")
#                         st.rerun()

#                 # --- Data Editor and Submission ---
#                 edited_df_local_copy = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR)
#                 if not st.session_state.ag_editor_data.empty:
#                     st.markdown("<h4>Review and Edit Extracted Data:</h4>", unsafe_allow_html=True)
#                     col_conf = {
#                         "audit_group_number": st.column_config.NumberColumn(disabled=True), "audit_circle_number": st.column_config.NumberColumn(disabled=True),
#                         "gstin": st.column_config.TextColumn(width="medium"), "trade_name": st.column_config.TextColumn(width="large"),
#                         "category": st.column_config.SelectboxColumn(options=[None] + VALID_CATEGORIES, required=False, width="small"),
#                         "total_amount_detected_overall_rs": st.column_config.NumberColumn("Total Detect (Rs)", format="%.2f", width="medium"),
#                         "total_amount_recovered_overall_rs": st.column_config.NumberColumn("Total Recover (Rs)", format="%.2f", width="medium"),
#                         "audit_para_number": st.column_config.NumberColumn("Para No.", format="%d", width="small", help="Integer only"),
#                         "audit_para_heading": st.column_config.TextColumn("Para Heading", width="xlarge"),
#                         "revenue_involved_lakhs_rs": st.column_config.NumberColumn("Rev. Involved (Lakhs)", format="%.2f", width="small"),
#                         "revenue_recovered_lakhs_rs": st.column_config.NumberColumn("Rev. Recovered (Lakhs)", format="%.2f", width="small"),
#                         "status_of_para": st.column_config.SelectboxColumn("Para Status", options=[None] + VALID_PARA_STATUSES, required=False, width="medium")}
#                     final_editor_col_conf = {k: v for k, v in col_conf.items() if k in DISPLAY_COLUMN_ORDER_EDITOR}
                    
#                     editor_key = f"data_editor_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_current_uploaded_file_name or 'no_file_active'}"
                    
#                     edited_df_local_copy = pd.DataFrame(st.data_editor(
#                         st.session_state.ag_editor_data.copy(),
#                         column_config=final_editor_col_conf, num_rows="dynamic",
#                         key=editor_key, use_container_width=True, hide_index=True, 
#                         height=min(len(st.session_state.ag_editor_data) * 45 + 70, 450) if not st.session_state.ag_editor_data.empty else 200
#                     ))

#                 submit_button_key = f"submit_btn_centralized_{st.session_state.ag_current_mcm_key}_{st.session_state.ag_current_uploaded_file_name or 'no_file_active'}"
#                 can_submit = not edited_df_local_copy.empty if not st.session_state.ag_editor_data.empty else False
#                 if st.button("Validate and Submit to Master Database", key=submit_button_key, use_container_width=True, disabled=not can_submit):
#                     df_from_editor = edited_df_local_copy.copy()

#                     # Drop completely empty rows
#                     df_to_submit = df_from_editor.dropna(how='all').reset_index(drop=True)

#                     if df_to_submit.empty and not df_from_editor.empty:
#                         st.error("Submission failed: Only empty rows were found. Please fill in the details.")
#                     else:
#                         # Check for missing data in essential columns
#                         required_cols = ['gstin', 'trade_name', 'audit_para_heading']
#                         missing_required = df_to_submit[required_cols].isnull().any(axis=1)

#                         if missing_required.any():
#                             st.error("Submission failed: At least one row is missing required information (e.g., GSTIN, Trade Name, or Para Heading). Please complete all fields.")
#                         else:
#                             df_to_submit["audit_group_number"] = st.session_state.audit_group_no
#                             df_to_submit["audit_circle_number"] = calculate_audit_circle(st.session_state.audit_group_no)

#                             num_cols_to_convert = ["total_amount_detected_overall_rs", "total_amount_recovered_overall_rs", "audit_para_number", "revenue_involved_lakhs_rs", "revenue_recovered_lakhs_rs"]
#                             for nc in num_cols_to_convert:
#                                 if nc in df_to_submit.columns: df_to_submit[nc] = pd.to_numeric(df_to_submit[nc], errors='coerce')
                            
#                             st.session_state.ag_validation_errors = validate_data_for_sheet(df_to_submit)
   
#                             if not st.session_state.ag_validation_errors:
#                                 if not st.session_state.ag_pdf_drive_url: 
#                                     st.error("PDF Drive URL missing. This indicates the initial PDF upload with extraction failed. Please re-extract data."); st.stop()

#                                 with st.spinner("Submitting to Master DAR Database..."):
#                                     rows_for_sheet = []; ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                                     mcm_period_str = f"{mcm_info_current['month_name']} {mcm_info_current['year']}"
                                    
#                                     final_df_for_sheet_upload = df_to_submit.copy()
#                                     for sheet_col_name in SHEET_DATA_COLUMNS_ORDER:
#                                         if sheet_col_name not in final_df_for_sheet_upload.columns:
#                                             final_df_for_sheet_upload[sheet_col_name] = None
                                    
#                                     final_df_for_sheet_upload["audit_group_number"] = st.session_state.audit_group_no
#                                     final_df_for_sheet_upload["audit_circle_number"] = calculate_audit_circle(st.session_state.audit_group_no)
                                    
#                                     for _, r_data_submit in final_df_for_sheet_upload.iterrows():
#                                         # Updated to include MCM Period in the row
#                                         sheet_row = [r_data_submit.get(col) for col in SHEET_DATA_COLUMNS_ORDER] + [st.session_state.ag_pdf_drive_url, ts, mcm_period_str]
#                                         rows_for_sheet.append(sheet_row)
                                    
#                                     if rows_for_sheet:
#                                         # Use centralized append function (no spreadsheet_id parameter needed)
#                                         if append_to_spreadsheet(sheets_service, rows_for_sheet):
#                                             st.success("Data submitted successfully to Master DAR Database!"); st.balloons(); time.sleep(1)
#                                             st.session_state.ag_current_uploaded_file_obj = None; st.session_state.ag_current_uploaded_file_name = None
#                                             st.session_state.ag_editor_data = pd.DataFrame(columns=DISPLAY_COLUMN_ORDER_EDITOR); st.session_state.ag_pdf_drive_url = None
#                                             st.session_state.ag_validation_errors = []; st.session_state.ag_uploader_key_suffix += 1
#                                             st.rerun()
#                                         else: st.error("Failed to append to Master DAR Database.")
#                                     else: st.error("No data rows to submit.")
#                             else:
#                                 st.error("Validation Failed! Correct errors.");
#                                 if st.session_state.ag_validation_errors: st.subheader("⚠️ Validation Errors:"); [st.warning(f"- {err}") for err in st.session_state.ag_validation_errors]
#             elif not period_select_map_rev: st.info("No MCM periods available.")

#     # ========================== VIEW MY UPLOADED DARS TAB ==========================
#     elif selected_tab == "View My Uploaded DARs":
#         st.markdown("<h3>My Uploaded DARs</h3>", unsafe_allow_html=True)
#         st.info("📊 Viewing data from the centralized Master DAR Database.")
        
#         if not mcm_periods_all: 
#             st.info("No MCM periods found.")
#         else:
#             view_period_opts_map = {k: f"{p.get('month_name')} {p.get('year')}" for k, p in sorted(mcm_periods_all.items(), key=lambda x: x[0], reverse=True) if p.get('month_name') and p.get('year')}
#             if not view_period_opts_map and mcm_periods_all: 
#                 st.warning("Some MCM periods have incomplete data.")
#             if not view_period_opts_map: 
#                 st.info("No valid MCM periods to view.")
#             else:
#                 sel_view_key = st.selectbox("Select MCM Period", options=list(view_period_opts_map.keys()), format_func=lambda k: view_period_opts_map[k], key="ag_view_sel_centralized")
#                 if sel_view_key and sheets_service:
#                     mcm_period_str = view_period_opts_map[sel_view_key]
                    
#                     with st.spinner("Loading uploads from Master Database..."): 
#                         df_sheet_all = read_from_spreadsheet(sheets_service)
                    
#                     if df_sheet_all is not None and not df_sheet_all.empty:
#                         # Filter by audit group and MCM period
#                         if "Audit Group Number" in df_sheet_all.columns:
#                             df_sheet_all["Audit Group Number"] = df_sheet_all["Audit Group Number"].astype(str)
#                             my_uploads = df_sheet_all[df_sheet_all["Audit Group Number"] == str(st.session_state.audit_group_no)]
                            
#                             # Further filter by MCM Period if column exists
#                             if 'MCM Period' in my_uploads.columns:
#                                 my_uploads = my_uploads[my_uploads['MCM Period'] == mcm_period_str]
                            
#                             if not my_uploads.empty:
#                                 st.markdown(f"<h4>Your Uploads for {mcm_period_str}:</h4>", unsafe_allow_html=True)
#                                 my_uploads_disp = my_uploads.copy()
#                                 if "DAR PDF URL" in my_uploads_disp.columns:
#                                     my_uploads_disp['DAR PDF URL Links'] = my_uploads_disp["DAR PDF URL"].apply(lambda x: f'<a href="{x}" target="_blank">View PDF</a>' if pd.notna(x) and str(x).startswith("http") else "No Link")
                                
#                                 cols_to_view_final = [
#                                     "Audit Circle Number", "GSTIN", "Trade Name", "Category",
#                                     "Total Amount Detected (Overall Rs)", "Total Amount Recovered (Overall Rs)",
#                                     "Audit Para Number", "Audit Para Heading", "Status of para",
#                                     "Revenue Involved (Lakhs Rs)", "Revenue Recovered (Lakhs Rs)",
#                                     "DAR PDF URL Links",
#                                     "Record Created Date"
#                                 ]
#                                 existing_cols_to_display = [c for c in cols_to_view_final if c in my_uploads_disp.columns]
                                
#                                 if not existing_cols_to_display:
#                                     st.warning("No relevant columns found to display for your uploads. Please check sheet structure.")
#                                 else:
#                                     st.markdown(my_uploads_disp[existing_cols_to_display].to_html(escape=False, index=False), unsafe_allow_html=True)
#                             else: 
#                                 st.info(f"No DARs by you for {mcm_period_str}.")
#                         else: 
#                             st.warning("Sheet missing 'Audit Group Number' column or data malformed.")
#                     elif df_sheet_all is None: 
#                         st.error("Error reading Master DAR Database for viewing.")
#                     else: 
#                         st.info(f"No data in Master DAR Database for {mcm_period_str}.")
#                 elif not sheets_service and sel_view_key: 
#                     st.error("Google Sheets service unavailable.")

#     # ========================== DELETE MY DAR ENTRIES TAB ==========================
#     elif selected_tab == "Delete My DAR Entries":
#         st.markdown("<h3>Delete My Uploaded DAR Entries</h3>", unsafe_allow_html=True)
#         st.info("⚠️ This action is irreversible. Deletion removes entries from the Master DAR Database; the PDF in centralized storage will remain.")
        
#         if not mcm_periods_all: 
#             st.info("No MCM periods found.")
#         else:
#             del_period_opts_map = {k: f"{p.get('month_name')} {p.get('year')}" for k, p in sorted(mcm_periods_all.items(), key=lambda x: x[0], reverse=True) if p.get('month_name') and p.get('year')}
#             if not del_period_opts_map and mcm_periods_all: 
#                 st.warning("Some MCM periods have incomplete data.")
#             if not del_period_opts_map: 
#                 st.info("No valid MCM periods to manage entries.")
#             else:
#                 sel_del_key = st.selectbox("Select MCM Period", options=list(del_period_opts_map.keys()), format_func=lambda k: del_period_opts_map[k], key="ag_del_sel_centralized")
#                 if sel_del_key and sheets_service:
#                     mcm_period_str = del_period_opts_map[sel_del_key]
#                     del_sheet_gid = 0
#                     try: 
#                         sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=MASTER_DAR_DATABASE_SHEET_ID).execute()
#                         del_sheet_gid = sheet_metadata.get('sheets', [{}])[0].get('properties', {}).get('sheetId', 0)
#                     except Exception as e_gid: 
#                         st.error(f"Could not get sheet GID: {e_gid}"); st.stop()

#                     with st.spinner("Loading entries from Master Database..."): 
#                         df_all_del_data = read_from_spreadsheet(sheets_service)
                        
#                     if df_all_del_data is not None and not df_all_del_data.empty:
#                         if 'Audit Group Number' in df_all_del_data.columns:
#                             df_all_del_data['Audit Group Number'] = df_all_del_data['Audit Group Number'].astype(str)
#                             my_entries_del = df_all_del_data[df_all_del_data['Audit Group Number'] == str(st.session_state.audit_group_no)].copy()
                            
#                             # Further filter by MCM Period if column exists
#                             if 'MCM Period' in my_entries_del.columns:
#                                 my_entries_del = my_entries_del[my_entries_del['MCM Period'] == mcm_period_str]
                            
#                             my_entries_del['original_data_index'] = my_entries_del.index 

#                             if not my_entries_del.empty:
#                                 st.markdown(f"<h4>Your Uploads in {mcm_period_str} (Select to delete):</h4>", unsafe_allow_html=True)
#                                 del_options_disp = ["--Select an entry to delete--"]; st.session_state.ag_deletable_map.clear()
#                                 for _, del_row in my_entries_del.iterrows():
#                                     del_ident = f"TN: {str(del_row.get('Trade Name', 'N/A'))[:20]} | Para: {del_row.get('Audit Para Number', 'N/A')} | Date: {del_row.get('Record Created Date', 'N/A')}"
#                                     del_options_disp.append(del_ident)
#                                     st.session_state.ag_deletable_map[del_ident] = {
#                                         "original_df_index": del_row['original_data_index'],
#                                         "Trade Name": str(del_row.get('Trade Name')),
#                                         "Audit Para Number": str(del_row.get('Audit Para Number')),
#                                         "Record Created Date": str(del_row.get('Record Created Date')),
#                                         "DAR PDF URL": str(del_row.get('DAR PDF URL'))
#                                     }
                                
#                                 sel_entry_del_str = st.selectbox("Select Entry:", options=del_options_disp, key=f"del_box_centralized_{sel_del_key}")
#                                 if sel_entry_del_str != "--Select an entry to delete--":
#                                     entry_info_to_delete = st.session_state.ag_deletable_map.get(sel_entry_del_str)
#                                     if entry_info_to_delete is not None :
#                                         orig_idx_to_del = entry_info_to_delete["original_df_index"]
#                                         st.warning(f"Confirm Deletion: TN: **{entry_info_to_delete.get('Trade Name')}**, Para: **{entry_info_to_delete.get('Audit Para Number')}**")
#                                         with st.form(key=f"del_form_centralized_{orig_idx_to_del}"):
#                                             pwd = st.text_input("Password:", type="password", key=f"del_pwd_centralized_{orig_idx_to_del}")
#                                             if st.form_submit_button("Yes, Delete This Entry"):
#                                                 if pwd == USER_CREDENTIALS.get(st.session_state.username):
#                                                     if delete_spreadsheet_rows(sheets_service, del_sheet_gid, [orig_idx_to_del]): 
#                                                         st.success("Entry deleted from Master Database."); time.sleep(1); st.rerun()
#                                                     else: st.error("Failed to delete from Master Database.")
#                                                 else: st.error("Incorrect password.")
#                                     else: st.error("Could not identify selected entry. Please refresh and re-select.")
#                             else: st.info(f"You have no entries in {mcm_period_str} to delete.")
#                         else: st.warning("Sheet missing 'Audit Group Number' column.")
#                     elif df_all_del_data is None: st.error("Error reading Master Database for deletion.")
#                     else: st.info(f"No data in Master Database for {mcm_period_str}.")
#                 elif not sheets_service and sel_del_key: st.error("Google Sheets service unavailable.")

#     st.markdown("</div>", unsafe_allow_html=True)
