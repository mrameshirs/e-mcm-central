# google_utils.py
import streamlit as st
import os
import json
from io import BytesIO
import pandas as pd
import math # Added for ceil, though not directly used here, good to have if needed

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload, MediaIoBaseDownload

from config import SCOPES, MASTER_DRIVE_FOLDER_NAME, MCM_PERIODS_FILENAME_ON_DRIVE

def get_google_services():
    creds = None
    try:
        creds_dict = st.secrets["google_credentials"]
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=SCOPES
        )
    except KeyError:
        st.error("Google credentials not found in Streamlit secrets. Ensure 'google_credentials' are set.")
        return None, None
    except Exception as e:
        st.error(f"Failed to load service account credentials from secrets: {e}")
        return None, None

    if not creds: return None, None

    try:
        drive_service = build('drive', 'v3', credentials=creds)
        sheets_service = build('sheets', 'v4', credentials=creds)
        return drive_service, sheets_service
    except HttpError as error:
        st.error(f"An error occurred initializing Google services: {error}")
        return None, None
    except Exception as e:
        st.error(f"An unexpected error with Google services: {e}")
        return None, None

def find_drive_item_by_name(drive_service, name, mime_type=None, parent_id=None):
    query = f"name = '{name}' and trashed = false"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    try:
        response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = response.get('files', [])
        if items:
            return items[0].get('id')
    except HttpError as error:
        st.warning(f"Error searching for '{name}' in Drive: {error}. This might be okay if the item is to be created.")
    except Exception as e:
        st.warning(f"Unexpected error searching for '{name}' in Drive: {e}")
    return None

def set_public_read_permission(drive_service, file_id):
    try:
        permission = {'type': 'anyone', 'role': 'reader'}
        drive_service.permissions().create(fileId=file_id, body=permission).execute()
    except HttpError as error:
        st.warning(f"Could not set public read permission for file ID {file_id}: {error}.")
    except Exception as e:
        st.warning(f"Unexpected error setting public permission for file ID {file_id}: {e}")

def create_drive_folder(drive_service, folder_name, parent_id=None):
    try:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]

        folder = drive_service.files().create(body=file_metadata, fields='id, webViewLink').execute()
        folder_id = folder.get('id')
        if folder_id:
            set_public_read_permission(drive_service, folder_id)
        return folder_id, folder.get('webViewLink')
    except HttpError as error:
        st.error(f"An error occurred creating Drive folder '{folder_name}': {error}")
        return None, None
    except Exception as e:
        st.error(f"Unexpected error creating Drive folder '{folder_name}': {e}")
        return None, None

def initialize_drive_structure(drive_service):
    master_id = st.session_state.get('master_drive_folder_id')
    if not master_id:
        master_id = find_drive_item_by_name(drive_service, MASTER_DRIVE_FOLDER_NAME,
                                            'application/vnd.google-apps.folder')
        if not master_id:
            st.info(f"Master folder '{MASTER_DRIVE_FOLDER_NAME}' not found on Drive, attempting to create it...")
            master_id, _ = create_drive_folder(drive_service, MASTER_DRIVE_FOLDER_NAME, parent_id=None)
            if master_id:
                st.success(f"Master folder '{MASTER_DRIVE_FOLDER_NAME}' created successfully.")
            else:
                st.error(f"Fatal: Failed to create master folder '{MASTER_DRIVE_FOLDER_NAME}'. Cannot proceed.")
                return False
        st.session_state.master_drive_folder_id = master_id

    if not st.session_state.master_drive_folder_id:
        st.error("Master Drive folder ID could not be established. Cannot proceed.")
        return False

    mcm_file_id = st.session_state.get('mcm_periods_drive_file_id')
    if not mcm_file_id:
        mcm_file_id = find_drive_item_by_name(drive_service, MCM_PERIODS_FILENAME_ON_DRIVE,
                                              parent_id=st.session_state.master_drive_folder_id)
        if mcm_file_id:
            st.session_state.mcm_periods_drive_file_id = mcm_file_id
    return True

def load_mcm_periods(drive_service):
    mcm_periods_file_id = st.session_state.get('mcm_periods_drive_file_id')
    if not mcm_periods_file_id:
        if st.session_state.get('master_drive_folder_id'):
            mcm_periods_file_id = find_drive_item_by_name(drive_service, MCM_PERIODS_FILENAME_ON_DRIVE,
                                                          parent_id=st.session_state.master_drive_folder_id)
            st.session_state.mcm_periods_drive_file_id = mcm_periods_file_id
        else:
            return {}

    if mcm_periods_file_id:
        try:
            request = drive_service.files().get_media(fileId=mcm_periods_file_id)
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            return json.load(fh)
        except HttpError as error:
            if error.resp.status == 404:
                st.session_state.mcm_periods_drive_file_id = None
            else:
                st.error(f"Error loading '{MCM_PERIODS_FILENAME_ON_DRIVE}' from Drive: {error}")
            return {}
        except json.JSONDecodeError:
            st.error(f"Error decoding JSON from '{MCM_PERIODS_FILENAME_ON_DRIVE}'. File might be corrupted.")
            return {}
        except Exception as e:
            st.error(f"Unexpected error loading '{MCM_PERIODS_FILENAME_ON_DRIVE}': {e}")
            return {}
    return {}

def save_mcm_periods(drive_service, periods_data):
    master_folder_id = st.session_state.get('master_drive_folder_id')
    if not master_folder_id:
        st.error("Master Drive folder ID not set. Cannot save MCM periods configuration to Drive.")
        return False

    mcm_periods_file_id = st.session_state.get('mcm_periods_drive_file_id')
    file_content = json.dumps(periods_data, indent=4).encode('utf-8')
    fh = BytesIO(file_content)
    media_body = MediaIoBaseUpload(fh, mimetype='application/json', resumable=True)

    try:
        if mcm_periods_file_id:
            file_metadata_update = {'name': MCM_PERIODS_FILENAME_ON_DRIVE}
            drive_service.files().update(
                fileId=mcm_periods_file_id,
                body=file_metadata_update,
                media_body=media_body,
                fields='id, name'
            ).execute()
        else:
            file_metadata_create = {'name': MCM_PERIODS_FILENAME_ON_DRIVE, 'parents': [master_folder_id]}
            new_file = drive_service.files().create(
                body=file_metadata_create,
                media_body=media_body,
                fields='id, name'
            ).execute()
            st.session_state.mcm_periods_drive_file_id = new_file.get('id')
        return True
    except HttpError as error:
        st.error(f"Error saving '{MCM_PERIODS_FILENAME_ON_DRIVE}' to Drive: {error}")
        return False
    except Exception as e:
        st.error(f"Unexpected error saving '{MCM_PERIODS_FILENAME_ON_DRIVE}': {e}")
        return False

def upload_to_drive(drive_service, file_content_or_path, folder_id, filename_on_drive):
    try:
        file_metadata = {'name': filename_on_drive, 'parents': [folder_id]}
        media_body = None

        if isinstance(file_content_or_path, str) and os.path.exists(file_content_or_path):
            media_body = MediaFileUpload(file_content_or_path, mimetype='application/pdf', resumable=True)
        elif isinstance(file_content_or_path, bytes): # Handle bytes directly
            fh = BytesIO(file_content_or_path)
            media_body = MediaIoBaseUpload(fh, mimetype='application/pdf', resumable=True)
        elif isinstance(file_content_or_path, BytesIO): # Handle already created BytesIO
            file_content_or_path.seek(0) # Ensure cursor is at the beginning
            media_body = MediaIoBaseUpload(file_content_or_path, mimetype='application/pdf', resumable=True)
        else:
            st.error(f"Unsupported file content type for Google Drive upload: {type(file_content_or_path)}")
            return None, None

        if media_body is None: # Should be caught by the else above, but as a safeguard
            st.error("Media body for upload could not be prepared.")
            return None, None

        request = drive_service.files().create(
            body=file_metadata,
            media_body=media_body,
            fields='id, webViewLink' # Request webViewLink for direct access
        )
        file = request.execute()
        file_id = file.get('id')
        if file_id:
            set_public_read_permission(drive_service, file_id) # Optional: make file publicly readable
        return file_id, file.get('webViewLink')
    except HttpError as error:
        st.error(f"An API error occurred uploading to Drive: {error}")
        return None, None
    except Exception as e:
        st.error(f"An unexpected error in upload_to_drive: {e}")
        return None, None

def create_spreadsheet(sheets_service, drive_service, title, parent_folder_id=None):
    try:
        spreadsheet_body = {'properties': {'title': title}}
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet_body,
                                                           fields='spreadsheetId,spreadsheetUrl').execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')

        if spreadsheet_id and drive_service:
            set_public_read_permission(drive_service, spreadsheet_id) # Optional
            if parent_folder_id: # Move spreadsheet to the specified folder
                file = drive_service.files().get(fileId=spreadsheet_id, fields='parents').execute()
                previous_parents = ",".join(file.get('parents'))
                drive_service.files().update(fileId=spreadsheet_id,
                                             addParents=parent_folder_id,
                                             removeParents=previous_parents,
                                             fields='id, parents').execute()
        return spreadsheet_id, spreadsheet.get('spreadsheetUrl')
    except HttpError as error:
        st.error(f"An error occurred creating Spreadsheet: {error}")
        return None, None
    except Exception as e:
        st.error(f"An unexpected error occurred creating Spreadsheet: {e}")
        return None, None

def append_to_spreadsheet(sheets_service, spreadsheet_id, values_to_append):
    try:
        body = {'values': values_to_append}
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        first_sheet_title = sheets[0].get("properties", {}).get("title", "Sheet1")

        # Check if header exists
        range_to_check_header = f"{first_sheet_title}!A1:N1" # Check up to N (14th column)
        result_header_check = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_to_check_header
        ).execute()
        header_row_in_sheet = result_header_check.get('values', [])

        if not header_row_in_sheet: # No header at all, create it
            header_to_write = [[
                "Audit Group Number", "Audit Circle Number", "GSTIN", "Trade Name", "Category",
                "Total Amount Detected (Overall Rs)", "Total Amount Recovered (Overall Rs)",
                "Audit Para Number", "Audit Para Heading",
                "Revenue Involved (Lakhs Rs)", "Revenue Recovered (Lakhs Rs)", "Status of para",
                "DAR PDF URL", "Record Created Date"
            ]]
            sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{first_sheet_title}!A1", # Start at A1
                valueInputOption='USER_ENTERED',
                body={'values': header_to_write}
            ).execute()

        # Append data rows
        append_result = sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{first_sheet_title}!A1", # Appends after the last row with data in this range
            valueInputOption='USER_ENTERED',
            body=body # values_to_append should not include header
        ).execute()
        return append_result
    except HttpError as error:
        st.error(f"An error occurred appending to Spreadsheet: {error}")
        return None
    except Exception as e:
        st.error(f"Unexpected error appending to Spreadsheet: {e}")
        return None

def read_from_spreadsheet(sheets_service, spreadsheet_id, sheet_name="Sheet1"):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name  # Read the whole sheet
        ).execute()
        values = result.get('values', [])

        if not values:
            return pd.DataFrame() # Return empty DataFrame if sheet is empty

        expected_cols_header = [ # This is the current, correct 14-column header
            "Audit Group Number", "Audit Circle Number", "GSTIN", "Trade Name", "Category",
            "Total Amount Detected (Overall Rs)", "Total Amount Recovered (Overall Rs)",
            "Audit Para Number", "Audit Para Heading",
            "Revenue Involved (Lakhs Rs)", "Revenue Recovered (Lakhs Rs)", "Status of para",
            "DAR PDF URL", "Record Created Date"
        ]

        header_in_sheet = values[0]
        data_rows = values[1:]

        if not data_rows : # Only header or empty after header
            if header_in_sheet == expected_cols_header:
                return pd.DataFrame(columns=expected_cols_header) # Correct header, no data
            else: # Potentially incorrect header, or just some other content
                 # Try to return what's there, might be messy, or return empty with expected if too different
                if len(header_in_sheet) > 5 : # Heuristic: if it looks somewhat like a header
                    return pd.DataFrame(columns=header_in_sheet)
                return pd.DataFrame(columns=expected_cols_header) # Fallback to expected if header is very short/unlikely

        num_cols_in_header = len(header_in_sheet)
        num_cols_in_first_data_row = len(data_rows[0]) if data_rows else 0 # Check first data row

        if header_in_sheet == expected_cols_header:
            # Ideal case: Header matches expected.
            # Ensure all data rows have a consistent number of columns. Pad if necessary.
            processed_data_rows = []
            for row in data_rows:
                if len(row) < len(expected_cols_header):
                    processed_data_rows.append(row + [None] * (len(expected_cols_header) - len(row)))
                elif len(row) > len(expected_cols_header):
                    processed_data_rows.append(row[:len(expected_cols_header)])
                else:
                    processed_data_rows.append(row)
            return pd.DataFrame(processed_data_rows, columns=header_in_sheet)

        elif num_cols_in_first_data_row == len(expected_cols_header):
            # Data structure matches expected 14 columns, but header in sheet might be old/different.
            # Prioritize using expected_cols_header for the DataFrame.
            st.warning(f"Spreadsheet header mismatched ({num_cols_in_header} cols), but data rows appear to have the current expected {len(expected_cols_header)} columns. Applying current headers.")
            # Pad/truncate all data rows to match expected_cols_header length
            standardized_data_rows = []
            for row in data_rows:
                if len(row) < len(expected_cols_header):
                    standardized_data_rows.append(row + [None] * (len(expected_cols_header) - len(row)))
                elif len(row) > len(expected_cols_header):
                    standardized_data_rows.append(row[:len(expected_cols_header)])
                else:
                    standardized_data_rows.append(row)
            return pd.DataFrame(standardized_data_rows, columns=expected_cols_header)

        elif num_cols_in_header == num_cols_in_first_data_row:
            # Header is different from expected, but consistent with data. Use sheet's header.
            #st.warning(f"Spreadsheet header ({num_cols_in_header} cols) differs from expected ({len(expected_cols_header)} cols), but is consistent with data rows. Using header from sheet: {header_in_sheet}")
            return pd.DataFrame(data_rows, columns=header_in_sheet)
        else:
            # Significant mismatch, e.g. header is 12, data is 14.
            # This was the problematic case. Try to use expected_cols_header if data matches it.
            error_message = (f"Spreadsheet structure conflict: Header has {num_cols_in_header} columns, "
                             f"first data row has {num_cols_in_first_data_row} columns. "
                             f"Expected {len(expected_cols_header)} columns based on current app version.")
            st.error(error_message)
            # Fallback: return raw values, which might lead to issues upstream, or an empty DF with expected cols.
            # For safety, let's try to build a DataFrame with expected columns and fill with what we can.
            st.info("Attempting to load data with current expected columns. Data might be misaligned.")
            try:
                # Pad/truncate all data rows to match expected_cols_header length
                standardized_data_rows_fallback = []
                for row_idx, row_val in enumerate(data_rows):
                    new_row = [None] * len(expected_cols_header)
                    for i in range(min(len(row_val), len(expected_cols_header))):
                        new_row[i] = row_val[i]
                    standardized_data_rows_fallback.append(new_row)
                return pd.DataFrame(standardized_data_rows_fallback, columns=expected_cols_header)
            except Exception as fallback_e:
                st.error(f"Fallback data loading also failed: {fallback_e}")
                return pd.DataFrame(columns=expected_cols_header) # Empty DF with correct columns

    except HttpError as error:
        st.error(f"An API error occurred reading from Spreadsheet: {error}")
        return pd.DataFrame(columns=expected_cols_header) # Return empty DF with expected structure
    except Exception as e:
        st.error(f"Unexpected error reading from Spreadsheet: {e}")
        return pd.DataFrame(columns=expected_cols_header) # Return empty DF with expected structure

def delete_spreadsheet_rows(sheets_service, spreadsheet_id, sheet_id_gid, row_indices_to_delete):
    # row_indices_to_delete are 0-based indices of the *data* rows (DataFrame iloc from read_from_spreadsheet)
    if not row_indices_to_delete:
        return True
    requests = []
    # Sort in descending order to avoid index shifting issues during deletion
    for data_row_index in sorted(row_indices_to_delete, reverse=True):
        # Sheet API uses 0-based indexing for rows *within the specified range*,
        # but deleteDimension needs 0-based index relative to start of sheet if sheetId is used.
        # If header is row 0 in API terms, data row 0 is sheet row 1.
        # The 'startIndex' for deleteDimension is 0-based and exclusive of the header if sheet data starts from row 1 (0-indexed) after header.
        # Assuming read_from_spreadsheet gives data starting from what would be sheet row index 1 (if header is 0).
        # So, if `data_row_index` is 0 (first data row), it means the 2nd row in the sheet (1-indexed), which is row index 1 for the API.
        sheet_row_start_index = data_row_index + 1 # If data starts at physical row 2 (index 1)
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id_gid,
                    "dimension": "ROWS",
                    "startIndex": sheet_row_start_index, # This is the 0-based index of the row in the sheet (header is 0)
                    "endIndex": sheet_row_start_index + 1
                }
            }
        })
    if requests:
        try:
            body = {'requests': requests}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body).execute()
            return True
        except HttpError as error:
            st.error(f"An error occurred deleting rows from Spreadsheet: {error}")
            return False
        except Exception as e:
            st.error(f"Unexpected error deleting rows: {e}")
            return False
    return True# # google_utils.py
def update_spreadsheet_from_df(sheets_service, spreadsheet_id, df_to_write):
    """
    Clears the first sheet in a spreadsheet and updates it with data from a pandas DataFrame.

    Args:
        sheets_service: The authenticated Google Sheets service object.
        spreadsheet_id (str): The ID of the spreadsheet to update.
        df_to_write (pd.DataFrame): The DataFrame containing the new data.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # Get the title of the first sheet, which is the target for clearing and updating
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        first_sheet_title = sheet_metadata['sheets'][0]['properties']['title']

        # Step 1: Clear the entire sheet to remove old data
        clear_range = f"{first_sheet_title}"
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range
        ).execute()

        # Step 2: Prepare the DataFrame for writing
        # Replace NaN/NaT values with empty strings, as the API handles them better
        df_prepared = df_to_write.fillna('')
        # Convert the DataFrame (including headers) to a list of lists
        values_to_write = [df_prepared.columns.values.tolist()] + df_prepared.values.tolist()

        # Step 3: Write the new data to the sheet starting from cell A1
        update_range = f"{first_sheet_title}!A1"
        body = {'values': values_to_write}
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_range,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        return True

    except HttpError as error:
        st.error(f"An API error occurred while updating the Spreadsheet: {error}")
        return False
    except Exception as e:
        st.error(f"An unexpected error occurred while updating the Spreadsheet: {e}")
        return False
