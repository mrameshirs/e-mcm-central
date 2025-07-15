# gemini_utils.py
import streamlit as st
import json
import time
import google.generativeai as genai
from models import ParsedDARReport # Ensure models.py is in the same directory or installable

def get_structured_data_with_gemini(api_key: str, text_content: str, max_retries=2) -> ParsedDARReport:
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        return ParsedDARReport(parsing_errors="Gemini API Key not configured.")
    if text_content.startswith("Error processing PDF with pdfplumber:") or \
            text_content.startswith("Error in preprocess_pdf_text_"):
        return ParsedDARReport(parsing_errors=text_content)

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash-latest')

    prompt = f"""
    You are an expert GST audit report analyst. Based on the following FULL text from a Departmental Audit Report (DAR),
    where all text from all pages, including tables, is provided, extract the specified information
    and structure it as a JSON object. Focus on identifying narrative sections for audit para details,
    even if they are intermingled with tabular data. Notes like "[INFO: ...]" in the text are for context only.

    The JSON object should follow this structure precisely:
    {{
      "header": {{
        "audit_group_number": "integer or null (e.g., if 'Group-VI' or 'Gr 6', extract 6; must be between 1 and 30)",
        "gstin": "string or null",
        "trade_name": "string or null",
        "category": "string ('Large', 'Medium', 'Small') or null",
        "total_amount_detected_overall_rs": "float or null (numeric value in Rupees)",
        "total_amount_recovered_overall_rs": "float or null (numeric value in Rupees)"
      }},
      "audit_paras": [
        {{
          "audit_para_number": "integer or null (primary number from para heading, e.g., for 'Para-1...' use 1; must be between 1 and 50)",
          "audit_para_heading": "string or null (the descriptive title of the para)",
          "revenue_involved_lakhs_rs": "float or null (numeric value in Lakhs of Rupees, e.g., Rs. 50,000 becomes 0.5)",
          "revenue_recovered_lakhs_rs": "float or null (numeric value in Lakhs of Rupees)",
          "status_of_para": "string or null (Possible values: 'Agreed and Paid', 'Agreed yet to pay', 'Partially agreed and paid', 'Partially agreed, yet to paid', 'Not agreed')"
        }}
      ],
      "parsing_errors": "string or null (any notes about parsing issues, or if extraction is incomplete)"
    }}

    Key Instructions:
    1.  Header Information: Extract `audit_group_number` (as integer 1-30, e.g., 'Group-VI' becomes 6), `gstin`, `trade_name`, `category`, `total_amount_detected_overall_rs`, `total_amount_recovered_overall_rs`.
    2.  Audit Paras: Identify each distinct para. Extract `audit_para_number` (as integer 1-50), `audit_para_heading`, `revenue_involved_lakhs_rs` (converted to Lakhs), `revenue_recovered_lakhs_rs` (converted to Lakhs), and `status_of_para`.
    3.  For `status_of_para`, strictly choose from: 'Agreed and Paid', 'Agreed yet to pay', 'Partially agreed and paid', 'Partially agreed, yet to paid', 'Not agreed'. If the status is unclear or different, use null.
    4.  Use null for missing values. Monetary values as float.
    5.  If no audit paras found, `audit_paras` should be an empty list [].

    DAR Text Content:
    --- START OF DAR TEXT ---
    {text_content}
    --- END OF DAR TEXT ---

    Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
    """

    attempt = 0
    last_exception = None
    while attempt <= max_retries:
        attempt += 1
        try:
            response = model.generate_content(prompt)
            cleaned_response_text = response.text.strip()
            if cleaned_response_text.startswith("```json"):
                cleaned_response_text = cleaned_response_text[7:]
            elif cleaned_response_text.startswith("`json"):
                cleaned_response_text = cleaned_response_text[6:]
            if cleaned_response_text.endswith("```"): cleaned_response_text = cleaned_response_text[:-3]

            if not cleaned_response_text:
                error_message = f"Gemini returned an empty response on attempt {attempt}."
                last_exception = ValueError(error_message)
                if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
                time.sleep(1 + attempt);
                continue

            json_data = json.loads(cleaned_response_text)
            if "header" not in json_data or "audit_paras" not in json_data:
                error_message = f"Gemini response (Attempt {attempt}) missing 'header' or 'audit_paras' key. Response: {cleaned_response_text[:500]}"
                last_exception = ValueError(error_message)
                if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
                time.sleep(1 + attempt);
                continue

            parsed_report = ParsedDARReport(**json_data)
            return parsed_report
        except json.JSONDecodeError as e:
            raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
            error_message = f"Gemini output (Attempt {attempt}) was not valid JSON: {e}. Response: '{raw_response_text[:1000]}...'"
            last_exception = e
            if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
            time.sleep(attempt * 2)
        except Exception as e:
            raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
            error_message = f"Error (Attempt {attempt}) during Gemini/Pydantic: {type(e).__name__} - {e}. Response: {raw_response_text[:500]}"
            last_exception = e
            if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
            time.sleep(attempt * 2)
    return ParsedDARReport(
        parsing_errors=f"Gemini call failed after {max_retries + 1} attempts. Last error: {last_exception}")
    # # gemini_utils.py
# import streamlit as st
# import json
# import time
# import google.generativeai as genai
# from models import ParsedDARReport # Ensure models.py is in the same directory or installable

# def get_structured_data_with_gemini(api_key: str, text_content: str, max_retries=2) -> ParsedDARReport:
#     if not api_key or api_key == "YOUR_API_KEY_HERE":
#         return ParsedDARReport(parsing_errors="Gemini API Key not configured.")
#     if text_content.startswith("Error processing PDF with pdfplumber:") or \
#             text_content.startswith("Error in preprocess_pdf_text_"):
#         return ParsedDARReport(parsing_errors=text_content)

#     genai.configure(api_key=api_key)
#     model = genai.GenerativeModel('gemini-1.5-flash-latest')

#     prompt = f"""
#     You are an expert GST audit report analyst. Based on the following FULL text from a Departmental Audit Report (DAR),
#     where all text from all pages, including tables, is provided, extract the specified information
#     and structure it as a JSON object. Focus on identifying narrative sections for audit para details,
#     even if they are intermingled with tabular data. Notes like "[INFO: ...]" in the text are for context only.

#     The JSON object should follow this structure precisely:
#     {{
#       "header": {{
#         "audit_group_number": "integer or null (e.g., if 'Group-VI' or 'Gr 6', extract 6; must be between 1 and 30)",
#         "gstin": "string or null",
#         "trade_name": "string or null",
#         "category": "string ('Large', 'Medium', 'Small') or null",
#         "total_amount_detected_overall_rs": "float or null (numeric value in Rupees)",
#         "total_amount_recovered_overall_rs": "float or null (numeric value in Rupees)"
#       }},
#       "audit_paras": [
#         {{
#           "audit_para_number": "integer or null (primary number from para heading, e.g., for 'Para-1...' use 1; must be between 1 and 50)",
#           "audit_para_heading": "string or null (the descriptive title of the para)",
#           "revenue_involved_lakhs_rs": "float or null (numeric value in Lakhs of Rupees, e.g., Rs. 50,000 becomes 0.5)",
#           "revenue_recovered_lakhs_rs": "float or null (numeric value in Lakhs of Rupees)"
#         }}
#       ],
#       "parsing_errors": "string or null (any notes about parsing issues, or if extraction is incomplete)"
#     }}

#     Key Instructions:
#     1.  Header Information: Extract `audit_group_number` (as integer 1-30, e.g., 'Group-VI' becomes 6), `gstin`, `trade_name`, `category`, `total_amount_detected_overall_rs`, `total_amount_recovered_overall_rs`.
#     2.  Audit Paras: Identify each distinct para. Extract `audit_para_number` (as integer 1-50), `audit_para_heading`, `revenue_involved_lakhs_rs` (converted to Lakhs), `revenue_recovered_lakhs_rs` (converted to Lakhs).
#     3.  Use null for missing values. Monetary values as float.
#     4.  If no audit paras found, `audit_paras` should be an empty list [].

#     DAR Text Content:
#     --- START OF DAR TEXT ---
#     {text_content}
#     --- END OF DAR TEXT ---

#     Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
#     """

#     attempt = 0
#     last_exception = None
#     while attempt <= max_retries:
#         attempt += 1
#         try:
#             response = model.generate_content(prompt)
#             cleaned_response_text = response.text.strip()
#             if cleaned_response_text.startswith("```json"):
#                 cleaned_response_text = cleaned_response_text[7:]
#             elif cleaned_response_text.startswith("`json"):
#                 cleaned_response_text = cleaned_response_text[6:]
#             if cleaned_response_text.endswith("```"): cleaned_response_text = cleaned_response_text[:-3]

#             if not cleaned_response_text:
#                 error_message = f"Gemini returned an empty response on attempt {attempt}."
#                 last_exception = ValueError(error_message)
#                 if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
#                 time.sleep(1 + attempt);
#                 continue

#             json_data = json.loads(cleaned_response_text)
#             if "header" not in json_data or "audit_paras" not in json_data:
#                 error_message = f"Gemini response (Attempt {attempt}) missing 'header' or 'audit_paras' key. Response: {cleaned_response_text[:500]}"
#                 last_exception = ValueError(error_message)
#                 if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
#                 time.sleep(1 + attempt);
#                 continue

#             parsed_report = ParsedDARReport(**json_data)
#             return parsed_report
#         except json.JSONDecodeError as e:
#             raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
#             error_message = f"Gemini output (Attempt {attempt}) was not valid JSON: {e}. Response: '{raw_response_text[:1000]}...'"
#             last_exception = e
#             if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
#             time.sleep(attempt * 2)
#         except Exception as e:
#             raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
#             error_message = f"Error (Attempt {attempt}) during Gemini/Pydantic: {type(e).__name__} - {e}. Response: {raw_response_text[:500]}"
#             last_exception = e
#             if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
#             time.sleep(attempt * 2)
#     return ParsedDARReport(
#         parsing_errors=f"Gemini call failed after {max_retries + 1} attempts. Last error: {last_exception}")
