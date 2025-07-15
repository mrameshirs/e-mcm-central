# ui_pco.py - Complete Updated for Centralized Approach
import streamlit as st
import datetime
import time
import pandas as pd
import plotly.express as px
from streamlit_option_menu import option_menu
import math
import html
from io import BytesIO
from urllib.parse import urlparse, parse_qs

# PDF manipulation libraries
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors
from reportlab.lib.units import inch
from PyPDF2 import PdfWriter, PdfReader
from reportlab.pdfgen import canvas

from google_utils import (
    load_mcm_periods, save_mcm_periods, upload_to_drive,
    append_to_spreadsheet, read_from_spreadsheet, update_spreadsheet_from_df,
    verify_sheets_access
)
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from config import USER_CREDENTIALS, MASTER_DAR_DATABASE_SHEET_ID

# --- Helper Functions for MCM Agenda ---
def format_inr(n):
    """Formats a number into the Indian numbering system."""
    try:
        n = int(n)
    except (ValueError, TypeError):
        return "0"
    
    if n < 0:
        return '-' + format_inr(-n)
    if n == 0:
        return "0"
    
    s = str(n)
    if len(s) <= 3:
        return s
    
    s_last_three = s[-3:]
    s_remaining = s[:-3]
    
    groups = []
    while len(s_remaining) > 2:
        groups.append(s_remaining[-2:])
        s_remaining = s_remaining[:-2]
    
    if s_remaining:
        groups.append(s_remaining)
    
    groups.reverse()
    result = ','.join(groups) + ',' + s_last_three
    return result

def get_file_id_from_drive_url(url: str) -> str | None:
    if not url or not isinstance(url, str):
        return None
    parsed_url = urlparse(url)
    if 'drive.google.com' in parsed_url.netloc:
        if '/file/d/' in parsed_url.path:
            try:
                return parsed_url.path.split('/file/d/')[1].split('/')[0]
            except IndexError:
                pass
        query_params = parse_qs(parsed_url.query)
        if 'id' in query_params:
            return query_params['id'][0]
    return None

def create_cover_page_pdf(buffer, title_text, subtitle_text):
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=1.5*inch, bottomMargin=1.5*inch, leftMargin=1*inch, rightMargin=1*inch)
    styles = getSampleStyleSheet()
    story = []
    title_style = ParagraphStyle('AgendaCoverTitle', parent=styles['h1'], fontName='Helvetica-Bold', fontSize=28, alignment=TA_CENTER, textColor=colors.HexColor("#dc3545"), spaceBefore=1*inch, spaceAfter=0.3*inch)
    story.append(Paragraph(title_text, title_style))
    story.append(Spacer(1, 0.3*inch))
    subtitle_style = ParagraphStyle('AgendaCoverSubtitle', parent=styles['h2'], fontName='Helvetica', fontSize=16, alignment=TA_CENTER, textColor=colors.darkslategray, spaceAfter=2*inch)
    story.append(Paragraph(subtitle_text, subtitle_style))
    doc.build(story)
    buffer.seek(0)
    return buffer

def create_high_value_paras_pdf(buffer, df_high_value_paras_data):
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=0.75*inch, rightMargin=0.75*inch, topMargin=0.75*inch, bottomMargin=0.75*inch)
    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph("<b>High-Value Audit Paras (&gt; ₹5 Lakhs Detection)</b>", styles['h1']))
    story.append(Spacer(1, 0.2*inch))
    table_data_hv = [[Paragraph("<b>Audit Group</b>", styles['Normal']), Paragraph("<b>Para No.</b>", styles['Normal']),
                      Paragraph("<b>Para Title</b>", styles['Normal']), Paragraph("<b>Detected (₹)</b>", styles['Normal']),
                      Paragraph("<b>Recovered (₹)</b>", styles['Normal'])]]
    for _, row_hv in df_high_value_paras_data.iterrows():
        detected_val = row_hv.get('Revenue Involved (Lakhs Rs)', 0) * 100000
        recovered_val = row_hv.get('Revenue Recovered (Lakhs Rs)', 0) * 100000
        table_data_hv.append([
            Paragraph(html.escape(str(row_hv.get("Audit Group Number", "N/A"))), styles['Normal']),
            Paragraph(html.escape(str(row_hv.get("Audit Para Number", "N/A"))), styles['Normal']),
            Paragraph(html.escape(str(row_hv.get("Audit Para Heading", "N/A"))[:100]), styles['Normal']),
            Paragraph(format_inr(detected_val), styles['Normal']),
            Paragraph(format_inr(recovered_val), styles['Normal'])])

    col_widths_hv = [1*inch, 0.7*inch, 3*inch, 1.4*inch, 1.4*inch]
    hv_table = Table(table_data_hv, colWidths=col_widths_hv)
    hv_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#343a40")), ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('ALIGN', (3,1), (-1,-1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10), ('TOPPADDING', (0,0), (-1,-1), 4), ('BOTTOMPADDING', (0,1), (-1,-1), 4),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)]))
    story.append(hv_table)
    doc.build(story)
    buffer.seek(0)
    return buffer

def calculate_audit_circle_agenda(audit_group_number_val):
    try:
        agn = int(audit_group_number_val)
        if 1 <= agn <= 30:
            return math.ceil(agn / 3.0)
        return 0
    except (ValueError, TypeError, AttributeError):
        return 0

def pco_dashboard(drive_service, sheets_service):
    st.markdown("<div class='sub-header'>Planning & Coordination Officer Dashboard</div>", unsafe_allow_html=True)
    
    # Verify access to sheets on load
    if not verify_sheets_access(sheets_service):
        st.error("Cannot access required Google Sheets. Please check permissions.")
        return
    
    mcm_periods = load_mcm_periods(sheets_service)

    with st.sidebar:
        try:
            st.image("logo.png", width=80)
        except Exception as e:
            st.sidebar.warning(f"Could not load logo.png: {e}")
            st.sidebar.markdown("*(Logo)*")

        st.markdown(f"**User:** {st.session_state.username}")
        st.markdown(f"**Role:** {st.session_state.role}")
        if st.button("Logout", key="pco_logout_centralized", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.username = ""
            st.session_state.role = ""
            st.session_state.drive_structure_initialized = False
            keys_to_clear = ['period_to_delete', 'show_delete_confirm', 'num_paras_to_show_pco']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        st.markdown("---")

    selected_tab = option_menu(
        menu_title=None,
        options=["Create MCM Period", "Manage MCM Periods", "View Uploaded Reports", "MCM Agenda", "Visualizations"],
        icons=["calendar-plus-fill", "sliders", "eye-fill", "journal-richtext", "bar-chart-fill"],
        menu_icon="gear-wide-connected", 
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "5px !important", "background-color": "#e9ecef"},
            "icon": {"color": "#007bff", "font-size": "20px"},
            "nav-link": {"font-size": "16px", "text-align": "center", "margin": "0px", "--hover-color": "#d1e7fd"},
            "nav-link-selected": {"background-color": "#007bff", "color": "white"},
        })

    st.markdown("<div class='card'>", unsafe_allow_html=True)

    # ========================== CREATE MCM PERIOD TAB ==========================
    if selected_tab == "Create MCM Period":
        st.markdown("<h3>Create New MCM Period</h3>", unsafe_allow_html=True)
        st.info("📁 All MCM periods will use the centralized folder and database for DAR uploads.")
        
        current_year = datetime.datetime.now().year
        years = list(range(current_year - 1, current_year + 3))
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        
        col1, col2 = st.columns(2)
        with col1:
            selected_year = st.selectbox("Select Year", options=years, index=years.index(current_year), key="pco_year_create_centralized")
        with col2:
            selected_month_name = st.selectbox("Select Month", options=months, index=datetime.datetime.now().month - 1, key="pco_month_create_centralized")
        
        selected_month_num = months.index(selected_month_name) + 1
        period_key = f"{selected_year}-{selected_month_num:02d}"

        mcm_periods_local_copy_create = mcm_periods.copy()

        if period_key in mcm_periods_local_copy_create:
            st.warning(f"MCM Period for {selected_month_name} {selected_year} already exists.")
        else:
            if st.button(f"Create MCM for {selected_month_name} {selected_year}", key="pco_btn_create_mcm_centralized", use_container_width=True):
                with st.spinner("Creating MCM period entry..."):
                    # No need to create folders/sheets - they already exist
                    mcm_periods_local_copy_create[period_key] = {
                        "year": selected_year, 
                        "month_num": selected_month_num, 
                        "month_name": selected_month_name,
                        "active": True
                    }
                    
                    if save_mcm_periods(sheets_service, mcm_periods_local_copy_create):
                        st.success(f"Successfully created MCM period for {selected_month_name} {selected_year}!")
                        st.info("📁 This period will use the centralized DAR upload folder and master database.")
                        st.balloons()
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("Failed to save MCM period configuration.")

    # ========================== MANAGE MCM PERIODS TAB ==========================
    elif selected_tab == "Manage MCM Periods":
        st.markdown("<h3>Manage Existing MCM Periods</h3>", unsafe_allow_html=True)
        st.info("📁 All periods use centralized storage. Only activation/deactivation is managed here.")
        st.markdown("<h5 style='color: green;'>Only the Months which are marked as 'Active' will be available in Audit group screen for uploading DARs.</h5>", unsafe_allow_html=True)
        
        mcm_periods_manage_local_copy = mcm_periods.copy()

        if not mcm_periods_manage_local_copy:
            st.info("No MCM periods created yet.")
        else:
            sorted_periods_keys_mng = sorted(mcm_periods_manage_local_copy.keys(), reverse=True)
            for period_key_for_manage in sorted_periods_keys_mng:
                data_for_manage = mcm_periods_manage_local_copy[period_key_for_manage]
                month_name_disp_mng = data_for_manage.get('month_name', 'Unknown Month')
                year_disp_mng = data_for_manage.get('year', 'Unknown Year')
                st.markdown(f"<h4>{month_name_disp_mng} {year_disp_mng}</h4>", unsafe_allow_html=True)
                
                col1_manage, col2_manage, col3_manage, col4_manage = st.columns([2, 2, 1, 2])
                with col1_manage:
                    st.markdown(f"<a href='https://drive.google.com/drive/folders/{st.session_state.centralized_dar_folder_id}' target='_blank'>📁 Centralized DAR Folder</a>", unsafe_allow_html=True)
                with col2_manage:
                    st.markdown(f"<a href='https://docs.google.com/spreadsheets/d/{MASTER_DAR_DATABASE_SHEET_ID}' target='_blank'>📊 Master Database</a>", unsafe_allow_html=True)
                with col3_manage:
                    is_active_current = data_for_manage.get("active", False)
                    new_status_current = st.checkbox("Active", value=is_active_current, key=f"active_centralized_{period_key_for_manage}")
                    if new_status_current != is_active_current:
                        mcm_periods_manage_local_copy[period_key_for_manage]["active"] = new_status_current
                        if save_mcm_periods(sheets_service, mcm_periods_manage_local_copy):
                            st.success(f"Status for {month_name_disp_mng} {year_disp_mng} updated.")
                            st.rerun()
                        else:
                            st.error("Failed to save updated status.")
                            mcm_periods_manage_local_copy[period_key_for_manage]["active"] = is_active_current
                with col4_manage:
                    if st.button("Delete Period Record", key=f"delete_mcm_btn_centralized_{period_key_for_manage}", type="secondary"):
                        st.session_state.period_to_delete = period_key_for_manage
                        st.session_state.show_delete_confirm = True
                        st.rerun()
                st.markdown("---")

            if st.session_state.get('show_delete_confirm') and st.session_state.get('period_to_delete'):
                period_key_to_delete_confirm = st.session_state.period_to_delete
                period_data_to_delete_confirm = mcm_periods_manage_local_copy.get(period_key_to_delete_confirm, {})
                with st.form(key=f"delete_confirm_form_centralized_{period_key_to_delete_confirm}"):
                    st.warning(f"Are you sure you want to delete the MCM period record for **{period_data_to_delete_confirm.get('month_name')} {period_data_to_delete_confirm.get('year')}**?")
                    st.info("**Note:** This only removes the period from tracking. DAR data in the centralized database will remain.")
                    pco_password_confirm_del = st.text_input("Enter your PCO password:", type="password", key=f"pco_pass_del_centralized_{period_key_to_delete_confirm}")
                    form_c1, form_c2 = st.columns(2)
                    with form_c1:
                        submitted_delete_final = st.form_submit_button("Yes, Delete Record from Tracking", use_container_width=True)
                    with form_c2:
                        if st.form_submit_button("Cancel", type="secondary", use_container_width=True):
                            st.session_state.show_delete_confirm = False
                            st.session_state.period_to_delete = None
                            st.rerun()
                    if submitted_delete_final:
                        if pco_password_confirm_del == USER_CREDENTIALS.get("planning_officer"):
                            del mcm_periods_manage_local_copy[period_key_to_delete_confirm]
                            if save_mcm_periods(sheets_service, mcm_periods_manage_local_copy):
                                st.success(f"MCM record for {period_data_to_delete_confirm.get('month_name')} {period_data_to_delete_confirm.get('year')} deleted from tracking.")
                            else:
                                st.error("Failed to save changes after deleting record locally.")
                            st.session_state.show_delete_confirm = False
                            st.session_state.period_to_delete = None
                            st.rerun()
                        else:
                            st.error("Incorrect password.")

    # ========================== VIEW UPLOADED REPORTS TAB ==========================
    elif selected_tab == "View Uploaded Reports":
        st.markdown("<h3>View Uploaded Reports Summary</h3>", unsafe_allow_html=True)
        st.info("📊 All data is stored in the centralized Master DAR Database.")
        
        # Load data from centralized database
        with st.spinner("Loading data from Master DAR Database..."):
            df_all_data = read_from_spreadsheet(sheets_service)
        
        if df_all_data is not None and not df_all_data.empty:
            # Filter by MCM Period if available
            mcm_period_filter = None
            if 'MCM Period' in df_all_data.columns:
                available_periods = df_all_data['MCM Period'].dropna().unique()
                if len(available_periods) > 0:
                    mcm_period_filter = st.selectbox(
                        "Filter by MCM Period (optional)", 
                        options=['All Periods'] + sorted(available_periods.tolist(), reverse=True),
                        key="pco_view_period_filter"
                    )
            
            # Apply filter if selected
            if mcm_period_filter and mcm_period_filter != 'All Periods':
                df_filtered = df_all_data[df_all_data['MCM Period'] == mcm_period_filter]
                st.info(f"Showing data for: {mcm_period_filter}")
            else:
                df_filtered = df_all_data
                st.info("Showing data for all MCM periods")
            
            if not df_filtered.empty:
                # Summary reports
                st.markdown("<h4>Summary of Uploads:</h4>", unsafe_allow_html=True)
                if 'Audit Group Number' in df_filtered.columns:
                    try:
                        df_filtered['Audit Group Number Numeric'] = pd.to_numeric(df_filtered['Audit Group Number'], errors='coerce')
                        df_summary_reports = df_filtered.dropna(subset=['Audit Group Number Numeric'])
                        
                        # Report 1: DARs per Group
                        dars_per_group_rep = df_summary_reports.groupby('Audit Group Number Numeric')['DAR PDF URL'].nunique().reset_index(name='DARs Uploaded')
                        st.write("**DARs Uploaded per Audit Group:**")
                        st.dataframe(dars_per_group_rep, use_container_width=True)
                        
                        # Report 2: Paras per Group
                        paras_per_group_rep = df_summary_reports.groupby('Audit Group Number Numeric').size().reset_index(name='Total Para Entries')
                        st.write("**Total Para Entries per Audit Group:**")
                        st.dataframe(paras_per_group_rep, use_container_width=True)
                        
                        # Report 3: DARs per Circle
                        if 'Audit Circle Number' in df_filtered.columns:
                            df_summary_reports['Audit Circle Number Numeric'] = pd.to_numeric(df_summary_reports['Audit Circle Number'], errors='coerce')
                            dars_per_circle_rep = df_summary_reports.dropna(subset=['Audit Circle Number Numeric']).groupby('Audit Circle Number Numeric')['DAR PDF URL'].nunique().reset_index(name='DARs Uploaded')
                            st.write("**DARs Uploaded per Audit Circle:**")
                            st.dataframe(dars_per_circle_rep, use_container_width=True)
                            
                        # Report 4: Para Status
                        if 'Status of para' in df_filtered.columns:
                            status_summary_rep = df_summary_reports['Status of para'].value_counts().reset_index(name='Count')
                            status_summary_rep.columns = ['Status of para', 'Count']
                            st.write("**Para Status Summary:**")
                            st.dataframe(status_summary_rep, use_container_width=True)
                        
                        st.markdown("<hr>", unsafe_allow_html=True)
                        
                        # Edit and save detailed data
                        st.markdown("<h4>Edit Detailed Data</h4>", unsafe_allow_html=True)
                        st.info("You can edit data in the table below. Click 'Save Changes' to update the Master DAR Database.", icon="✍️")

                        edited_df = st.data_editor(
                            df_filtered,
                            use_container_width=True,
                            hide_index=True,
                            num_rows="dynamic",
                            key="editor_centralized_master"
                        )

                        if st.button("Save Changes to Master Database", type="primary"):
                            with st.spinner("Saving changes to Master DAR Database..."):
                                success = update_spreadsheet_from_df(sheets_service, edited_df)
                                if success:
                                    st.success("Changes saved successfully to Master DAR Database!")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Failed to save changes. Please check the error message above.")

                    except Exception as e_rep_sum:
                        st.error(f"Error processing summary: {e_rep_sum}")
                else:
                    st.warning("Missing 'Audit Group Number' column for summary.")
                    st.dataframe(df_filtered, use_container_width=True)
            else:
                st.info("No data found for the selected filter.")
        elif df_all_data is None:
            st.error("Could not load data from the Master DAR Database.")
        else:
            st.info("No data in Master DAR Database yet.")

    # ========================== MCM AGENDA TAB ==========================
    elif selected_tab == "MCM Agenda":
        st.markdown("<h3>MCM Agenda Preparation</h3>", unsafe_allow_html=True)
        st.info("📊 Agenda will be generated from the centralized Master DAR Database.")
        
        # Load data from centralized database
        with st.spinner("Loading data from Master DAR Database..."):
            df_all_data = read_from_spreadsheet(sheets_service)
        
        if df_all_data is not None and not df_all_data.empty:
            # Filter by MCM Period for agenda
            if 'MCM Period' in df_all_data.columns:
                available_periods = df_all_data['MCM Period'].dropna().unique()
                if len(available_periods) > 0:
                    selected_period = st.selectbox(
                        "Select MCM Period for Agenda", 
                        options=sorted(available_periods.tolist(), reverse=True),
                        key="mcm_agenda_period_centralized"
                    )
                    
                    df_period_data = df_all_data[df_all_data['MCM Period'] == selected_period]
                    st.markdown(f"<h2 style='text-align: center; color: #007bff; font-size: 22pt; margin-bottom:10px;'>MCM Audit Paras for {selected_period}</h2>", unsafe_allow_html=True)
                    
                    # Process and display agenda
                    display_mcm_agenda_centralized(df_period_data, drive_service, sheets_service, selected_period)
                else:
                    st.warning("No MCM Period data found in the database.")
            else:
                st.warning("MCM Period column not found in the database. Please ensure data includes MCM Period information.")
        else:
            st.info("No data available in Master DAR Database for agenda generation.")

    # ========================== VISUALIZATIONS TAB ==========================
    elif selected_tab == "Visualizations":
        st.markdown("<h3>Data Visualizations</h3>", unsafe_allow_html=True)
        st.info("📊 Visualizations based on centralized Master DAR Database.")
        
        # Load data from centralized database
        with st.spinner("Loading data from Master DAR Database..."):
            df_all_data = read_from_spreadsheet(sheets_service)
        
        if df_all_data is not None and not df_all_data.empty:
            # Filter by MCM Period if available
            mcm_period_filter = None
            if 'MCM Period' in df_all_data.columns:
                available_periods = df_all_data['MCM Period'].dropna().unique()
                if len(available_periods) > 0:
                    mcm_period_filter = st.selectbox(
                        "Select MCM Period for Visualization", 
                        options=['All Periods'] + sorted(available_periods.tolist(), reverse=True),
                        key="pco_viz_period_filter"
                    )
            
            # Apply filter if selected
            if mcm_period_filter and mcm_period_filter != 'All Periods':
                df_viz_data = df_all_data[df_all_data['MCM Period'] == mcm_period_filter]
                st.info(f"Visualizing data for: {mcm_period_filter}")
            else:
                df_viz_data = df_all_data
                st.info("Visualizing data for all MCM periods")
            
            if not df_viz_data.empty:
                # Data cleaning and preparation
                viz_amount_cols = ['Total Amount Detected (Overall Rs)', 'Total Amount Recovered (Overall Rs)', 'Revenue Involved (Lakhs Rs)', 'Revenue Recovered (Lakhs Rs)']
                for v_col in viz_amount_cols:
                    if v_col in df_viz_data.columns:
                        df_viz_data[v_col] = df_viz_data[v_col].astype(str).str.replace(r'[^\d.]', '', regex=True)
                        df_viz_data[v_col] = pd.to_numeric(df_viz_data[v_col], errors='coerce').fillna(0)
                
                if 'Audit Group Number' in df_viz_data.columns:
                    df_viz_data['Audit Group Number'] = pd.to_numeric(df_viz_data['Audit Group Number'], errors='coerce').fillna(0).astype(int)

                # De-duplicate data for aggregated charts
                if 'DAR PDF URL' in df_viz_data.columns and df_viz_data['DAR PDF URL'].notna().any():
                    df_unique_reports = df_viz_data.drop_duplicates(subset=['DAR PDF URL']).copy()
                else:
                    st.warning("⚠️ 'DAR PDF URL' column not found. Chart sums might be inflated due to repeated values.")
                    df_unique_reports = df_viz_data.copy()

                # Convert amounts to Lakhs for visualization
                if 'Total Amount Detected (Overall Rs)' in df_unique_reports.columns:
                    df_unique_reports['Detection in Lakhs'] = df_unique_reports['Total Amount Detected (Overall Rs)'] / 100000.0
                if 'Total Amount Recovered (Overall Rs)' in df_unique_reports.columns:
                    df_unique_reports['Recovery in Lakhs'] = df_unique_reports['Total Amount Recovered (Overall Rs)'] / 100000.0

                # Summary metrics
                st.markdown("#### Performance Summary")
                num_dars = df_unique_reports['DAR PDF URL'].nunique() if 'DAR PDF URL' in df_unique_reports.columns else 0
                total_detected = df_unique_reports['Total Amount Detected (Overall Rs)'].sum() if 'Total Amount Detected (Overall Rs)' in df_unique_reports.columns else 0
                total_recovered = df_unique_reports['Total Amount Recovered (Overall Rs)'].sum() if 'Total Amount Recovered (Overall Rs)' in df_unique_reports.columns else 0

                col1, col2, col3 = st.columns(3)
                col1.metric(label="✅ No. of DARs Submitted", value=f"{num_dars}")
                col2.metric(label="💰 Total Revenue Involved", value=f"₹{total_detected/100000:.2f} Lakhs")
                col3.metric(label="🏆 Total Revenue Recovered", value=f"₹{total_recovered/100000:.2f} Lakhs")

                # Group performance metrics
                if 'Audit Group Number' in df_unique_reports.columns and df_unique_reports['Audit Group Number'].nunique() > 0:
                    dars_per_group = df_unique_reports[df_unique_reports['Audit Group Number'] > 0].groupby('Audit Group Number')['DAR PDF URL'].nunique()
                    
                    if not dars_per_group.empty:
                        max_dars_count = dars_per_group.max()
                        max_dars_group = dars_per_group.idxmax()
                        max_group_str = f"AG {max_dars_group} ({max_dars_count} DARs)"
                    else:
                        max_group_str = "N/A"

                    # Assuming total audit groups are from 1 to 30
                    all_audit_groups = set(range(1, 31)) 
                    submitted_groups = set(dars_per_group.index)
                    zero_dar_groups = sorted(list(all_audit_groups - submitted_groups))
                    zero_dar_groups_str = ", ".join(map(str, zero_dar_groups)) if zero_dar_groups else "None"

                    st.markdown(f"**Maximum DARs by:** `{max_group_str}`")
                    st.markdown(f"**Audit Groups with Zero DARs:** `{zero_dar_groups_str}`")

                # Visualizations
                generate_centralized_visualizations(df_viz_data, df_unique_reports)
            else:
                st.info("No data found for the selected period.")
        else:
            st.info("No data available in Master DAR Database for visualizations.")

    st.markdown("</div>", unsafe_allow_html=True)

def display_mcm_agenda_centralized(df_period_data, drive_service, sheets_service, selected_period):
    """Enhanced MCM agenda display for centralized approach"""
    if df_period_data.empty:
        st.info("No data available for this MCM period.")
        return
    
    # Data preparation
    cols_to_convert_numeric = ['Audit Group Number', 'Audit Circle Number', 'Total Amount Detected (Overall Rs)', 
                               'Total Amount Recovered (Overall Rs)', 'Audit Para Number', 
                               'Revenue Involved (Lakhs Rs)', 'Revenue Recovered (Lakhs Rs)']
    for col_name in cols_to_convert_numeric:
        if col_name in df_period_data.columns:
            df_period_data[col_name] = df_period_data[col_name].astype(str).str.replace(r'[^\d.]', '', regex=True)
            df_period_data[col_name] = pd.to_numeric(df_period_data[col_name], errors='coerce').fillna(0)

    # Derive/Validate Audit Circle Number
    circle_col_to_use = 'Audit Circle Number'
    if 'Audit Circle Number' not in df_period_data.columns or not df_period_data['Audit Circle Number'].notna().any():
        if 'Audit Group Number' in df_period_data.columns and df_period_data['Audit Group Number'].notna().any():
            df_period_data['Derived Audit Circle Number'] = df_period_data['Audit Group Number'].apply(calculate_audit_circle_agenda).fillna(0).astype(int)
            circle_col_to_use = 'Derived Audit Circle Number'
        else:
            df_period_data['Derived Audit Circle Number'] = 0
            circle_col_to_use = 'Derived Audit Circle Number'
    else:
        df_period_data['Audit Circle Number'] = df_period_data['Audit Circle Number'].fillna(0).astype(int)

    # Display circle-wise agenda
    for circle_num in range(1, 11):
        circle_label = f"Audit Circle {circle_num}"
        df_circle_data = df_period_data[df_period_data[circle_col_to_use] == circle_num]

        if not df_circle_data.empty:
            expander_header_html = f"<div style='background-color:#007bff; color:white; padding:10px 15px; border-radius:5px; margin-top:12px; margin-bottom:3px; font-weight:bold; font-size:16pt;'>{html.escape(circle_label)}</div>"
            st.markdown(expander_header_html, unsafe_allow_html=True)
            
            with st.expander(f"View Details for {html.escape(circle_label)}", expanded=False):
                group_labels_list = []
                group_dfs_list = []
                min_grp = (circle_num - 1) * 3 + 1
                max_grp = circle_num * 3

                for grp_num in range(min_grp, max_grp + 1):
                    df_grp_data = df_circle_data[df_circle_data['Audit Group Number'] == grp_num]
                    if not df_grp_data.empty:
                        group_labels_list.append(f"Audit Group {grp_num}")
                        group_dfs_list.append(df_grp_data)
                
                if not group_labels_list:
                    st.write(f"No specific audit group data found within {circle_label}.")
                    continue
                
                group_tabs = st.tabs(group_labels_list)

                for i, group_tab in enumerate(group_tabs):
                    with group_tab:
                        df_current_grp = group_dfs_list[i]
                        unique_trade_names = df_current_grp.get('Trade Name', pd.Series(dtype='str')).dropna().unique()

                        if not unique_trade_names.any():
                            st.write("No trade names with DARs found for this group.")
                            continue
                        
                        st.markdown(f"**DARs for {group_labels_list[i]}:**")
                        session_key_selected_trade = f"selected_trade_{circle_num}_{group_labels_list[i].replace(' ','_')}"

                        for tn_idx, trade_name in enumerate(unique_trade_names):
                            trade_name_data = df_current_grp[df_current_grp['Trade Name'] == trade_name]
                            dar_pdf_url = None
                            if not trade_name_data.empty and 'DAR PDF URL' in trade_name_data.columns:
                                dar_pdf_url = trade_name_data['DAR PDF URL'].iloc[0]

                            cols_trade_display = st.columns([0.7, 0.3])
                            with cols_trade_display[0]:
                                if st.button(f"{trade_name}", key=f"tradebtn_agenda_centralized_{circle_num}_{i}_{tn_idx}", help=f"Toggle paras for {trade_name}", use_container_width=True):
                                    st.session_state[session_key_selected_trade] = None if st.session_state.get(session_key_selected_trade) == trade_name else trade_name
                            
                            with cols_trade_display[1]:
                                if pd.notna(dar_pdf_url) and dar_pdf_url.startswith("http"):
                                    st.link_button("View DAR PDF", dar_pdf_url, use_container_width=True, type="secondary")
                                else:
                                    st.caption("No PDF Link")

                            if st.session_state.get(session_key_selected_trade) == trade_name:
                                df_trade_paras = df_current_grp[df_current_grp['Trade Name'] == trade_name].copy()
                                
                                # Category and GSTIN info
                                taxpayer_category = "N/A"
                                taxpayer_gstin = "N/A"
                                if not df_trade_paras.empty:
                                    first_row = df_trade_paras.iloc[0]
                                    taxpayer_category = first_row.get('Category', 'N/A')
                                    taxpayer_gstin = first_row.get('GSTIN', 'N/A')
                                
                                category_color_map = {
                                    "Large": ("#f8d7da", "#721c24"),
                                    "Medium": ("#ffeeba", "#856404"),
                                    "Small": ("#d4edda", "#155724"),
                                    "N/A": ("#e2e3e5", "#383d41")
                                }
                                cat_bg_color, cat_text_color = category_color_map.get(taxpayer_category, ("#e2e3e5", "#383d41"))

                                info_cols = st.columns(2)
                                with info_cols[0]:
                                    st.markdown(f"""
                                    <div style="background-color: {cat_bg_color}; color: {cat_text_color}; padding: 4px 8px; border-radius: 5px; text-align: center; font-size: 0.9rem; margin-top: 5px;">
                                        <b>Category:</b> {html.escape(str(taxpayer_category))}
                                    </div>
                                    """, unsafe_allow_html=True)
                                with info_cols[1]:
                                    st.markdown(f"""
                                    <div style="background-color: #e9ecef; color: #495057; padding: 4px 8px; border-radius: 5px; text-align: center; font-size: 0.9rem; margin-top: 5px;">
                                        <b>GSTIN:</b> {html.escape(str(taxpayer_gstin))}
                                    </div>
                                    """, unsafe_allow_html=True)
                                
                                st.markdown(f"<h5 style='font-size:13pt; margin-top:20px; color:#154360;'>Gist of Audit Paras & MCM Decisions for: {html.escape(trade_name)}</h5>", unsafe_allow_html=True)
                                
                                # CSS for styling
                                st.markdown("""
                                    <style>
                                        .grid-header { font-weight: bold; background-color: #343a40; color: white; padding: 10px 5px; border-radius: 5px; text-align: center; }
                                        .cell-style { padding: 8px 5px; margin: 1px; border-radius: 5px; text-align: center; }
                                        .title-cell { background-color: #f0f2f6; text-align: left; padding-left: 10px;}
                                        .revenue-cell { background-color: #e8f5e9; font-weight: bold; }
                                        .status-cell { background-color: #e3f2fd; font-weight: bold; color: #800000; }
                                        .total-row { font-weight: bold; padding-top: 10px; }
                                    </style>
                                """, unsafe_allow_html=True)

                                col_proportions = (0.9, 5, 1.5, 1.5, 1.8, 2.5)
                                header_cols = st.columns(col_proportions)
                                headers = ['Para No.', 'Para Title', 'Detection (₹)', 'Recovery (₹)', 'Status', 'MCM Decision']
                                for col, header in zip(header_cols, headers):
                                    col.markdown(f"<div class='grid-header'>{header}</div>", unsafe_allow_html=True)
                                
                                decision_options = ['Para closed since recovered', 'Para deferred', 'Para to be pursued else issue SCN']
                                total_para_det_rs, total_para_rec_rs = 0, 0
                                
                                for index, row in df_trade_paras.iterrows():
                                    with st.container(border=True):
                                        para_num_str = str(int(row["Audit Para Number"])) if pd.notna(row["Audit Para Number"]) and row["Audit Para Number"] != 0 else "N/A"
                                        det_rs = (row.get('Revenue Involved (Lakhs Rs)', 0) * 100000) if pd.notna(row.get('Revenue Involved (Lakhs Rs)')) else 0
                                        rec_rs = (row.get('Revenue Recovered (Lakhs Rs)', 0) * 100000) if pd.notna(row.get('Revenue Recovered (Lakhs Rs)')) else 0
                                        total_para_det_rs += det_rs
                                        total_para_rec_rs += rec_rs
                                        status_text = html.escape(str(row.get("Status of para", "N/A")))
                                        para_title_text = f"<b>{html.escape(str(row.get('Audit Para Heading', 'N/A')))}</b>"
                                        
                                        default_index = 0
                                        if 'MCM Decision' in df_trade_paras.columns and pd.notna(row['MCM Decision']) and row['MCM Decision'] in decision_options:
                                            default_index = decision_options.index(row['MCM Decision'])
                                        
                                        row_cols = st.columns(col_proportions)
                                        row_cols[0].write(para_num_str)
                                        row_cols[1].markdown(f"<div class='cell-style title-cell'>{para_title_text}</div>", unsafe_allow_html=True)
                                        row_cols[2].markdown(f"<div class='cell-style revenue-cell'>{format_inr(det_rs)}</div>", unsafe_allow_html=True)
                                        row_cols[3].markdown(f"<div class='cell-style revenue-cell'>{format_inr(rec_rs)}</div>", unsafe_allow_html=True)
                                        row_cols[4].markdown(f"<div class='cell-style status-cell'>{status_text}</div>", unsafe_allow_html=True)
                                        
                                        decision_key = f"mcm_decision_{trade_name}_{para_num_str}_{index}"
                                        row_cols[5].selectbox("Decision", options=decision_options, index=default_index, key=decision_key, label_visibility="collapsed")
                                
                                st.markdown("---")
                                with st.container():
                                    total_cols = st.columns(col_proportions)
                                    total_cols[1].markdown("<div class='total-row' style='text-align:right;'>Total of Paras</div>", unsafe_allow_html=True)
                                    total_cols[2].markdown(f"<div class='total-row revenue-cell cell-style'>{format_inr(total_para_det_rs)}</div>", unsafe_allow_html=True)
                                    total_cols[3].markdown(f"<div class='total-row revenue-cell cell-style'>{format_inr(total_para_rec_rs)}</div>", unsafe_allow_html=True)

                                st.markdown("<br>", unsafe_allow_html=True)
                                
                                # Overall totals
                                total_overall_detection, total_overall_recovery = 0, 0
                                if not df_trade_paras.empty:
                                    detection_val = df_trade_paras['Total Amount Detected (Overall Rs)'].iloc[0]
                                    recovery_val = df_trade_paras['Total Amount Recovered (Overall Rs)'].iloc[0]
                                    total_overall_detection = 0 if pd.isna(detection_val) else detection_val
                                    total_overall_recovery = 0 if pd.isna(recovery_val) else recovery_val
                                
                                # Styled summary lines
                                detection_style = "background-color: #f8d7da; color: #721c24; font-weight: bold; padding: 10px; border-radius: 5px; font-size: 1.2em;"
                                recovery_style = "background-color: #d4edda; color: #155724; font-weight: bold; padding: 10px; border-radius: 5px; font-size: 1.2em;"
                                
                                st.markdown(f"<p style='{detection_style}'>Total Detection for {html.escape(trade_name)}: ₹ {format_inr(total_overall_detection)}</p>", unsafe_allow_html=True)
                                st.markdown(f"<p style='{recovery_style}'>Total Recovery for {html.escape(trade_name)}: ₹ {format_inr(total_overall_recovery)}</p>", unsafe_allow_html=True)
                                
                                st.markdown("<br>", unsafe_allow_html=True)
                                
                                if st.button("Save Decisions", key=f"save_decisions_{trade_name}", use_container_width=True, type="primary"):
                                    with st.spinner("Saving decisions..."):
                                        # Load current data
                                        current_df = read_from_spreadsheet(sheets_service)
                                        
                                        if 'MCM Decision' not in current_df.columns:
                                            current_df['MCM Decision'] = ""
                                        
                                        # Update decisions for this trade name
                                        for index, row in df_trade_paras.iterrows():
                                            para_num_str = str(int(row["Audit Para Number"])) if pd.notna(row["Audit Para Number"]) and row["Audit Para Number"] != 0 else "N/A"
                                            decision_key = f"mcm_decision_{trade_name}_{para_num_str}_{index}"
                                            selected_decision = st.session_state.get(decision_key, decision_options[0])
                                            
                                            # Find matching rows in the current dataframe
                                            mask = (
                                                (current_df['Trade Name'] == trade_name) &
                                                (current_df['MCM Period'] == selected_period) &
                                                (current_df['Audit Para Number'].astype(str) == para_num_str)
                                            )
                                            current_df.loc[mask, 'MCM Decision'] = selected_decision
                                        
                                        success = update_spreadsheet_from_df(sheets_service, current_df)
                                        
                                        if success:
                                            st.success("✅ Decisions saved successfully!")
                                        else:
                                            st.error("❌ Failed to save decisions. Check app logs for details.")
                                
                                st.markdown("<hr>", unsafe_allow_html=True)

    # PDF Compilation Button
    st.markdown("---")
    if st.button("Compile Full MCM Agenda PDF", key="compile_mcm_agenda_pdf_centralized", type="primary", help="Generates a comprehensive PDF.", use_container_width=True):
        if df_period_data.empty:
            st.error("No data available for the selected MCM period to compile into PDF.")
        else:
            compile_mcm_pdf_centralized(df_period_data, drive_service, selected_period)

def compile_mcm_pdf_centralized(df_period_data, drive_service, selected_period):
    """Compile MCM agenda PDF from centralized data"""
    status_message_area = st.empty()
    progress_bar = st.progress(0)
    
    with st.spinner("Preparing for PDF compilation..."):
        final_pdf_merger = PdfWriter()
        compiled_pdf_pages_count = 0
        
        # Filter and sort data for PDF
        df_for_pdf = df_period_data.dropna(subset=['DAR PDF URL', 'Trade Name']).copy()
        
        # Get unique DARs, sorted for consistent processing order
        unique_dars_to_process = df_for_pdf.sort_values(by=['Audit Circle Number', 'Trade Name', 'DAR PDF URL']).drop_duplicates(subset=['DAR PDF URL'])
        
        total_dars = len(unique_dars_to_process)
        dar_objects_for_merge_and_index = []
        
        if total_dars == 0:
            status_message_area.warning("No valid DARs with PDF URLs found to compile.")
            progress_bar.empty()
            st.stop()

        total_steps_for_pdf = 4 + (2 * total_dars)
        current_pdf_step = 0

        # Step 1: Pre-fetch DAR PDFs to count pages
        if drive_service:
            status_message_area.info(f"Pre-fetching {total_dars} DAR PDFs to count pages and prepare content...")
            for idx, dar_row in unique_dars_to_process.iterrows():
                current_pdf_step += 1
                dar_url_val = dar_row.get('DAR PDF URL')
                file_id_val = get_file_id_from_drive_url(dar_url_val)
                num_pages_val = 1  # Default in case of fetch failure
                reader_obj_val = None
                trade_name_val = dar_row.get('Trade Name', 'Unknown DAR')
                circle_val = f"Circle {int(dar_row.get('Audit Circle Number', 0))}"

                status_message_area.info(f"Step {current_pdf_step}/{total_steps_for_pdf}: Fetching DAR for {trade_name_val}...")
                if file_id_val:
                    try:
                        req_val = drive_service.files().get_media(fileId=file_id_val)
                        fh_val = BytesIO()
                        downloader = MediaIoBaseDownload(fh_val, req_val)
                        done = False
                        while not done:
                            status, done = downloader.next_chunk(num_retries=2)
                        fh_val.seek(0)
                        reader_obj_val = PdfReader(fh_val)
                        num_pages_val = len(reader_obj_val.pages) if reader_obj_val.pages else 1
                    except HttpError as he:
                        st.warning(f"PDF HTTP Error for {trade_name_val} ({dar_url_val}): {he}. Using placeholder.")
                    except Exception as e_fetch_val:
                        st.warning(f"PDF Read Error for {trade_name_val} ({dar_url_val}): {e_fetch_val}. Using placeholder.")

                dar_objects_for_merge_and_index.append({
                    'circle': circle_val,
                    'trade_name': trade_name_val,
                    'num_pages_in_dar': num_pages_val,
                    'pdf_reader': reader_obj_val,
                    'dar_url': dar_url_val
                })
                progress_bar.progress(current_pdf_step / total_steps_for_pdf)
        else:
            status_message_area.error("Google Drive service not available.")
            progress_bar.empty()
            st.stop()

    # Now compile with progress
    try:
        # Step 2: Cover Page
        current_pdf_step += 1
        status_message_area.info(f"Step {current_pdf_step}/{total_steps_for_pdf}: Generating Cover Page...")
        cover_buffer = BytesIO()
        create_cover_page_pdf(cover_buffer, f"Audit Paras for MCM {selected_period}", "Audit 1 Commissionerate Mumbai")
        cover_reader = PdfReader(cover_buffer)
        final_pdf_merger.append(cover_reader)
        compiled_pdf_pages_count += len(cover_reader.pages)
        progress_bar.progress(current_pdf_step / total_steps_for_pdf)

        # Step 3: High-Value Paras Table
        current_pdf_step += 1
        status_message_area.info(f"Step {current_pdf_step}/{total_steps_for_pdf}: Generating High-Value Paras Table...")
        df_hv_data = df_period_data[(df_period_data['Revenue Involved (Lakhs Rs)'].fillna(0) * 100000) > 500000].copy()
        df_hv_data.sort_values(by='Revenue Involved (Lakhs Rs)', ascending=False, inplace=True)
        hv_pages_count = 0
        if not df_hv_data.empty:
            hv_buffer = BytesIO()
            create_high_value_paras_pdf(hv_buffer, df_hv_data)
            hv_reader = PdfReader(hv_buffer)
            final_pdf_merger.append(hv_reader)
            hv_pages_count = len(hv_reader.pages)
        compiled_pdf_pages_count += hv_pages_count
        progress_bar.progress(current_pdf_step / total_steps_for_pdf)

        # Step 4: Index Page (simplified)
        current_pdf_step += 1
        status_message_area.info(f"Step {current_pdf_step}/{total_steps_for_pdf}: Generating Index Page...")
        # Skip complex index generation for now
        progress_bar.progress(current_pdf_step / total_steps_for_pdf)

        # Step 5: Merge actual DAR PDFs
        for i, dar_detail_info in enumerate(dar_objects_for_merge_and_index):
            current_pdf_step += 1
            status_message_area.info(f"Step {current_pdf_step}/{total_steps_for_pdf}: Merging DAR {i+1}/{total_dars} ({html.escape(dar_detail_info['trade_name'])})...")
            if dar_detail_info['pdf_reader']:
                final_pdf_merger.append(dar_detail_info['pdf_reader'])
            else:  # Placeholder
                ph_b = BytesIO()
                ph_d = SimpleDocTemplate(ph_b, pagesize=A4)
                ph_s = [Paragraph(f"Content for {html.escape(dar_detail_info['trade_name'])} (URL: {html.escape(dar_detail_info['dar_url'])}) failed to load.", getSampleStyleSheet()['Normal'])]
                ph_d.build(ph_s)
                ph_b.seek(0)
                final_pdf_merger.append(PdfReader(ph_b))
            progress_bar.progress(current_pdf_step / total_steps_for_pdf)

        # Step 6: Finalize PDF
        current_pdf_step += 1
        status_message_area.info(f"Step {current_pdf_step}/{total_steps_for_pdf}: Finalizing PDF...")
        output_pdf_final = BytesIO()
        final_pdf_merger.write(output_pdf_final)
        output_pdf_final.seek(0)
        progress_bar.progress(1.0)
        status_message_area.success("PDF Compilation Complete!")

        dl_filename = f"MCM_Agenda_{selected_period.replace(' ', '_')}_Compiled.pdf"
        st.download_button(label="⬇️ Download Compiled PDF Agenda", data=output_pdf_final, file_name=dl_filename, mime="application/pdf")

    except Exception as e_compile_outer:
        status_message_area.error(f"An error occurred during PDF compilation: {e_compile_outer}")
        import traceback
        st.error(traceback.format_exc())
    finally:
        import time
        time.sleep(0.5)  # Brief pause to ensure user sees final status
        status_message_area.empty()
        progress_bar.empty()

def generate_centralized_visualizations(df_viz_data, df_unique_reports):
    """Generate visualizations for centralized data"""
    
    # Para Status Distribution
    if 'Status of para' in df_viz_data.columns and df_viz_data['Status of para'].nunique() > 1:
        st.markdown("---")
        st.markdown("<h4>Para Status Distribution</h4>", unsafe_allow_html=True)
        status_counts = df_viz_data['Status of para'].value_counts().reset_index()
        status_counts.columns = ['Status of para', 'Count']
        fig_status = px.bar(status_counts, x='Status of para', y='Count', text_auto=True, title="Distribution of Para Statuses")
        fig_status.update_traces(textposition='outside', marker_color='teal')
        st.plotly_chart(fig_status, use_container_width=True)
    
    # Group-wise Performance
    if 'Audit Group Number' in df_unique_reports.columns and df_unique_reports['Audit Group Number'].nunique() > 1:
        st.markdown("---")
        st.markdown("<h4>Group-wise Performance</h4>", unsafe_allow_html=True)
        
        df_unique_reports['Audit Group Number Str'] = df_unique_reports['Audit Group Number'].astype(str)
        
        if 'Detection in Lakhs' in df_unique_reports.columns:
            detection_data = df_unique_reports.groupby('Audit Group Number Str')['Detection in Lakhs'].sum().reset_index().sort_values(by='Detection in Lakhs', ascending=False).head(5)
            if not detection_data.empty:
                st.write("**Top 5 Groups by Detection Amount (Lakhs ₹):**")
                fig_det = px.bar(detection_data, x='Audit Group Number Str', y='Detection in Lakhs', text_auto='.2f')
                fig_det.update_traces(textposition='outside', marker_color='indianred')
                st.plotly_chart(fig_det, use_container_width=True)
        
        if 'Recovery in Lakhs' in df_unique_reports.columns:
            recovery_data = df_unique_reports.groupby('Audit Group Number Str')['Recovery in Lakhs'].sum().reset_index().sort_values(by='Recovery in Lakhs', ascending=False).head(5)
            if not recovery_data.empty:
                st.write("**Top 5 Groups by Recovery Amount (Lakhs ₹):**")
                fig_rec = px.bar(recovery_data, x='Audit Group Number Str', y='Recovery in Lakhs', text_auto='.2f')
                fig_rec.update_traces(textposition='outside', marker_color='lightseagreen')
                st.plotly_chart(fig_rec, use_container_width=True)

    # Circle-wise Performance
    if 'Audit Circle Number' in df_unique_reports.columns:
        st.markdown("---")
        st.markdown("<h4>Circle-wise Performance</h4>", unsafe_allow_html=True)
        
        df_unique_reports['Circle Number Str'] = df_unique_reports['Audit Circle Number'].astype(str)
        
        if 'Detection in Lakhs' in df_unique_reports.columns:
            circle_detection_data = df_unique_reports.groupby('Circle Number Str')['Detection in Lakhs'].sum().reset_index().sort_values(by='Detection in Lakhs', ascending=False)
            if not circle_detection_data.empty:
                st.write("**Circle-wise Detection Amount (Lakhs ₹):**")
                fig_circle_det = px.bar(circle_detection_data, x='Circle Number Str', y='Detection in Lakhs', text_auto='.2f')
                fig_circle_det.update_traces(textposition='outside', marker_color='mediumseagreen')
                st.plotly_chart(fig_circle_det, use_container_width=True)

    # Treemap Visualizations
    if 'Detection in Lakhs' in df_unique_reports.columns and 'Trade Name' in df_unique_reports.columns:
        st.markdown("---")
        st.markdown("<h4>Detection Treemap by Trade Name</h4>", unsafe_allow_html=True)
        
        df_treemap_data = df_unique_reports[df_unique_reports['Detection in Lakhs'] > 0].copy()
        if not df_treemap_data.empty and 'Category' in df_treemap_data.columns:
            df_treemap_data['Category'] = df_treemap_data['Category'].fillna('Unknown')
            try:
                fig_treemap = px.treemap(
                    df_treemap_data, 
                    path=[px.Constant("All Detections"), 'Category', 'Trade Name'], 
                    values='Detection in Lakhs', 
                    color='Category',
                    hover_name='Trade Name',
                    color_discrete_map={
                        'Large': 'rgba(230, 57, 70, 0.8)', 
                        'Medium': 'rgba(241, 196, 15, 0.8)', 
                        'Small': 'rgba(26, 188, 156, 0.8)', 
                        'Unknown': 'rgba(149, 165, 166, 0.7)'
                    }
                )
                fig_treemap.update_layout(margin=dict(t=30, l=10, r=10, b=10))
                st.plotly_chart(fig_treemap, use_container_width=True)
            except Exception as e_treemap:
                st.error(f"Could not generate treemap: {e_treemap}")

    # Para-wise Performance
    st.markdown("---")
    st.markdown("<h4>Para-wise Performance</h4>", unsafe_allow_html=True)
    
    if 'num_paras_to_show_pco' not in st.session_state:
        st.session_state.num_paras_to_show_pco = 5
    
    n_paras_input = st.text_input("Enter N for Top N Paras (e.g., 5):", value=str(st.session_state.num_paras_to_show_pco), key="pco_n_paras_input_centralized")
    num_paras_show = st.session_state.num_paras_to_show_pco
    
    try:
        parsed_n = int(n_paras_input)
        if parsed_n < 1:
            num_paras_show = 5
            st.warning("N must be positive. Showing Top 5.", icon="⚠️")
        elif parsed_n > 50:
            num_paras_show = 50
            st.warning("N capped at 50. Showing Top 50.", icon="⚠️")
        else:
            num_paras_show = parsed_n
        st.session_state.num_paras_to_show_pco = num_paras_show
    except ValueError:
        if n_paras_input != str(st.session_state.num_paras_to_show_pco):
            st.warning(f"Invalid N ('{n_paras_input}'). Using: {num_paras_show}", icon="⚠️")
    
    # Filter out template/error rows
    df_paras_only = df_viz_data[
        df_viz_data['Audit Para Number'].notna() & 
        (~df_viz_data['Audit Para Heading'].astype(str).isin([
            "N/A - Header Info Only (Add Paras Manually)", 
            "Manual Entry Required", 
            "Manual Entry - PDF Error", 
            "Manual Entry - PDF Upload Failed"
        ]))
    ]
    
    if 'Revenue Involved (Lakhs Rs)' in df_paras_only.columns:
        top_det_paras = df_paras_only.nlargest(num_paras_show, 'Revenue Involved (Lakhs Rs)')
        if not top_det_paras.empty:
            st.write(f"**Top {num_paras_show} Detection Paras (by Revenue Involved):**")
            display_cols_det = ['Audit Group Number', 'Trade Name', 'Audit Para Number', 'Audit Para Heading', 'Revenue Involved (Lakhs Rs)', 'Status of para']
            existing_cols_det = [c for c in display_cols_det if c in top_det_paras.columns]
            st.dataframe(top_det_paras[existing_cols_det], use_container_width=True)
    
    if 'Revenue Recovered (Lakhs Rs)' in df_paras_only.columns:
        top_rec_paras = df_paras_only.nlargest(num_paras_show, 'Revenue Recovered (Lakhs Rs)')
        if not top_rec_paras.empty:
            st.write(f"**Top {num_paras_show} Recovery Paras (by Revenue Recovered):**")
            display_cols_rec = ['Audit Group Number', 'Trade Name', 'Audit Para Number', 'Audit Para Heading', 'Revenue Recovered (Lakhs Rs)', 'Status of para']
            existing_cols_rec = [c for c in display_cols_rec if c in top_rec_paras.columns]
            st.dataframe(top_rec_paras[existing_cols_rec], use_container_width=True)