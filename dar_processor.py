# dar_processor.py
import pdfplumber
import google.generativeai as genai
import json
from typing import List, Dict, Any
from models import ParsedDARReport, DARHeaderSchema, AuditParaSchema  # Using your models.py


def preprocess_pdf_text(pdf_path_or_bytes) -> str:
    """
    Extracts all text from all pages of the PDF using pdfplumber,
    attempting to preserve layout for better LLM understanding.
    """
    processed_text_parts = []
    try:
        with pdfplumber.open(pdf_path_or_bytes) as pdf:
            for i, page in enumerate(pdf.pages):
                # Using layout=True can help preserve the reading order and structure
                # which might be beneficial for the LLM.
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)

                if page_text is None:
                    page_text = f"[INFO: Page {i + 1} yielded no text directly]"
                else:
                    # Basic sanitization: replace "None" strings that might have been literally extracted
                    page_text = page_text.replace("None", "")

                processed_text_parts.append(f"\n--- PAGE {i + 1} ---\n{page_text}")

        full_text = "".join(processed_text_parts)
        # print(f"Full preprocessed text length: {len(full_text)}") # For debugging
        # print(full_text[:2000]) # Print snippet for debugging
        return full_text
    except Exception as e:
        error_msg = f"Error processing PDF with pdfplumber: {type(e).__name__} - {e}"
        print(error_msg)
        return error_msg


def get_structured_data_with_gemini(api_key: str, text_content: str) -> ParsedDARReport:
    """
    Calls Gemini API with the full PDF text and parses the response.
    """
    if text_content.startswith("Error processing PDF with pdfplumber:"):
        return ParsedDARReport(parsing_errors=text_content)

    genai.configure(api_key=api_key)
    # Using a model capable of handling potentially larger context and complex instructions.
    # 'gemini-1.5-flash-latest' is a good balance.
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
    1.  **Header Information (usually from first 1-3 pages):**
        - For `audit_group_number`: Extract the group number as an integer. Example: 'Group-VI' or 'Gr 6' becomes 6. Must be between 1 and 30. If not determinable as such, return null.
        - Extract `gstin`, `trade_name`, and `category`.
        - `total_amount_detected_overall_rs`: Grand total detection for the entire audit (in Rupees).
        - `total_amount_recovered_overall_rs`: Grand total recovery for the entire audit (in Rupees).
    2.  **Audit Paras (can appear on any page after initial header info):**
        - Identify each distinct audit para. They often start with "Para-X" or similar.
        - For `audit_para_number`: Extract the main number from the para heading as an integer (e.g., "Para-1..." or "Para 1." becomes 1). Must be an integer between 1 and 50.
        - Extract `audit_para_heading` (the descriptive title/summary of the para).
        - Extract "Revenue involved" specific to THAT para and convert it to LAKHS of Rupees (amount_in_rs / 100000.0).
        - Extract "Revenue recovered" specific to THAT para (e.g. from 'amount paid' or 'party contention') and convert it to LAKHS of Rupees.
        - Extract `status_of_para`. Strictly choose from: 'Agreed and Paid', 'Agreed yet to pay', 'Partially agreed and paid', 'Partially agreed, yet to paid', 'Not agreed'. If the status is unclear or different, use null.
    3.  If any field's value is not found or cannot be determined, use null for that field.
    4.  Ensure all monetary values are numbers (float).
    5.  The 'audit_paras' list should contain one object per para. If no paras found, provide an empty list [].

    DAR Text Content:
    --- START OF DAR TEXT ---
    {text_content}
    --- END OF DAR TEXT ---

    Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
    """

    print("\n--- Calling Gemini with simplified full text approach ---")
    # print(f"Prompt (first 500 chars):\n{prompt[:500]}...") # For debugging

    try:
        response = model.generate_content(prompt)

        cleaned_response_text = response.text.strip()
        if cleaned_response_text.startswith("```json"):
            cleaned_response_text = cleaned_response_text[7:]
        elif cleaned_response_text.startswith("`json"):
            cleaned_response_text = cleaned_response_text[6:]
        if cleaned_response_text.endswith("```"):
            cleaned_response_text = cleaned_response_text[:-3]

        if not cleaned_response_text:
            error_message = "Gemini returned an empty response."
            print(error_message)
            return ParsedDARReport(parsing_errors=error_message)

        json_data = json.loads(cleaned_response_text)
        parsed_report = ParsedDARReport(**json_data)  # Validation against your models.py
        print(f"Gemini call successful. Paras found: {len(parsed_report.audit_paras)}")
        if parsed_report.audit_paras:
            for idx, para_obj in enumerate(parsed_report.audit_paras):
                if not para_obj.audit_para_heading:
                    print(
                        f"  Note: Para {idx + 1} (Number: {para_obj.audit_para_number}) has a missing heading from Gemini.")
        return parsed_report
    except json.JSONDecodeError as e:
        raw_response_text = "No response text available"
        if 'response' in locals() and hasattr(response, 'text'):
            raw_response_text = response.text
        error_message = f"Gemini output was not valid JSON: {e}. Response: '{raw_response_text[:1000]}...'"
        print(error_message)
        return ParsedDARReport(parsing_errors=error_message)
    except Exception as e:
        raw_response_text = "No response text available"
        if 'response' in locals() and hasattr(response, 'text'):
            raw_response_text = response.text
        error_message = f"Error during Gemini/Pydantic: {type(e).__name__} - {e}. Response: {raw_response_text[:500]}"
        print(error_message)
        return ParsedDARReport(parsing_errors=error_message)
        # # dar_processor.py
# import pdfplumber
# import google.generativeai as genai
# import json
# from typing import List, Dict, Any
# from models import ParsedDARReport, DARHeaderSchema, AuditParaSchema  # Using your models.py


# def preprocess_pdf_text(pdf_path_or_bytes) -> str:
#     """
#     Extracts all text from all pages of the PDF using pdfplumber,
#     attempting to preserve layout for better LLM understanding.
#     """
#     processed_text_parts = []
#     try:
#         with pdfplumber.open(pdf_path_or_bytes) as pdf:
#             for i, page in enumerate(pdf.pages):
#                 # Using layout=True can help preserve the reading order and structure
#                 # which might be beneficial for the LLM.
#                 page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)

#                 if page_text is None:
#                     page_text = f"[INFO: Page {i + 1} yielded no text directly]"
#                 else:
#                     # Basic sanitization: replace "None" strings that might have been literally extracted
#                     page_text = page_text.replace("None", "")

#                 processed_text_parts.append(f"\n--- PAGE {i + 1} ---\n{page_text}")

#         full_text = "".join(processed_text_parts)
#         # print(f"Full preprocessed text length: {len(full_text)}") # For debugging
#         # print(full_text[:2000]) # Print snippet for debugging
#         return full_text
#     except Exception as e:
#         error_msg = f"Error processing PDF with pdfplumber: {type(e).__name__} - {e}"
#         print(error_msg)
#         return error_msg


# def get_structured_data_with_gemini(api_key: str, text_content: str) -> ParsedDARReport:
#     """
#     Calls Gemini API with the full PDF text and parses the response.
#     """
#     if text_content.startswith("Error processing PDF with pdfplumber:"):
#         return ParsedDARReport(parsing_errors=text_content)

#     genai.configure(api_key=api_key)
#     # Using a model capable of handling potentially larger context and complex instructions.
#     # 'gemini-1.5-flash-latest' is a good balance.
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
#     1.  **Header Information (usually from first 1-3 pages):**
#         - For `audit_group_number`: Extract the group number as an integer. Example: 'Group-VI' or 'Gr 6' becomes 6. Must be between 1 and 30. If not determinable as such, return null.
#         - Extract `gstin`, `trade_name`, and `category`.
#         - `total_amount_detected_overall_rs`: Grand total detection for the entire audit (in Rupees).
#         - `total_amount_recovered_overall_rs`: Grand total recovery for the entire audit (in Rupees).
#     2.  **Audit Paras (can appear on any page after initial header info):**
#         - Identify each distinct audit para. They often start with "Para-X" or similar.
#         - For `audit_para_number`: Extract the main number from the para heading as an integer (e.g., "Para-1..." or "Para 1." becomes 1). Must be an integer between 1 and 50.
#         - Extract `audit_para_heading` (the descriptive title/summary of the para).
#         - Extract "Revenue involved" specific to THAT para and convert it to LAKHS of Rupees (amount_in_rs / 100000.0).
#         - Extract "Revenue recovered" specific to THAT para (e.g. from 'amount paid' or 'party contention') and convert it to LAKHS of Rupees.
#     3.  If any field's value is not found or cannot be determined, use null for that field.
#     4.  Ensure all monetary values are numbers (float).
#     5.  The 'audit_paras' list should contain one object per para. If no paras found, provide an empty list [].

#     DAR Text Content:
#     --- START OF DAR TEXT ---
#     {text_content}
#     --- END OF DAR TEXT ---

#     Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
#     """

#     print("\n--- Calling Gemini with simplified full text approach ---")
#     # print(f"Prompt (first 500 chars):\n{prompt[:500]}...") # For debugging

#     try:
#         response = model.generate_content(prompt)

#         cleaned_response_text = response.text.strip()
#         if cleaned_response_text.startswith("```json"):
#             cleaned_response_text = cleaned_response_text[7:]
#         elif cleaned_response_text.startswith("`json"):
#             cleaned_response_text = cleaned_response_text[6:]
#         if cleaned_response_text.endswith("```"):
#             cleaned_response_text = cleaned_response_text[:-3]

#         if not cleaned_response_text:
#             error_message = "Gemini returned an empty response."
#             print(error_message)
#             return ParsedDARReport(parsing_errors=error_message)

#         json_data = json.loads(cleaned_response_text)
#         parsed_report = ParsedDARReport(**json_data)  # Validation against your models.py
#         print(f"Gemini call successful. Paras found: {len(parsed_report.audit_paras)}")
#         if parsed_report.audit_paras:
#             for idx, para_obj in enumerate(parsed_report.audit_paras):
#                 if not para_obj.audit_para_heading:
#                     print(
#                         f"  Note: Para {idx + 1} (Number: {para_obj.audit_para_number}) has a missing heading from Gemini.")
#         return parsed_report
#     except json.JSONDecodeError as e:
#         raw_response_text = "No response text available"
#         if 'response' in locals() and hasattr(response, 'text'):
#             raw_response_text = response.text
#         error_message = f"Gemini output was not valid JSON: {e}. Response: '{raw_response_text[:1000]}...'"
#         print(error_message)
#         return ParsedDARReport(parsing_errors=error_message)
#     except Exception as e:
#         raw_response_text = "No response text available"
#         if 'response' in locals() and hasattr(response, 'text'):
#             raw_response_text = response.text
#         error_message = f"Error during Gemini/Pydantic: {type(e).__name__} - {e}. Response: {raw_response_text[:500]}"
#         print(error_message)
#         return ParsedDARReport(parsing_errors=error_message)
# # # dar_processor.py
# # import pdfplumber
# # import google.generativeai as genai
# # import json
# # from typing import List, Dict, Any
# # from models import ParsedDARReport, DARHeaderSchema, AuditParaSchema  # Using your models.py
# #
# #
# # # This is the preprocess_pdf_text you provided in the last file upload
# # # (with refined table filtering for later pages)
# # def preprocess_pdf_text_variant_1_filtered(pdf_path_or_bytes, max_pages_for_tables=3) -> str:
# #     """
# #     Extracts text from PDF using pdfplumber.
# #     Formats tables as Markdown for the first `max_pages_for_tables`.
# #     For subsequent pages, attempts to intelligently filter out only dense tabular data,
# #     prioritizing preservation of narrative text.
# #     """
# #     processed_text_parts = []
# #     try:
# #         with pdfplumber.open(pdf_path_or_bytes) as pdf:
# #             for i, page in enumerate(pdf.pages):
# #                 page_number_for_log = i + 1
# #                 page_text_content = ""
# #
# #                 if i < max_pages_for_tables:
# #                     page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2)
# #                     if page_text_content is None: page_text_content = ""
# #
# #                     initial_page_table_settings = {
# #                         "vertical_strategy": "lines", "horizontal_strategy": "lines",
# #                         "snap_tolerance": 4, "join_tolerance": 4,
# #                         "min_words_vertical": 2, "min_words_horizontal": 2
# #                     }
# #                     tables = page.extract_tables(table_settings=initial_page_table_settings)
# #                     if tables:
# #                         page_text_content += f"\n\n--- Extracted Tables (Page {page_number_for_log}) Start ---\n"
# #                         for table_idx, table_data in enumerate(tables):
# #                             if table_data:
# #                                 page_text_content += f"\n--- Table {table_idx + 1} ---\n"
# #                                 header_row_data = table_data[0]
# #                                 if header_row_data:
# #                                     str_header_row = [str(cell) if cell is not None else "" for cell in header_row_data]
# #                                     page_text_content += "| " + " | ".join(str_header_row) + " |\n"
# #                                     page_text_content += "| " + " | ".join(["---"] * len(str_header_row)) + " |\n"
# #                                 for row_data in table_data[1:]:
# #                                     if row_data:
# #                                         str_row = [str(cell) if cell is not None else "" for cell in row_data]
# #                                         page_text_content += "| " + " | ".join(str_row) + " |\n"
# #                         page_text_content += f"--- Extracted Tables (Page {page_number_for_log}) End ---\n\n"
# #                 else:
# #                     later_page_table_finder_settings = {
# #                         "vertical_strategy": "lines", "horizontal_strategy": "lines",
# #                         "snap_tolerance": 5, "join_tolerance": 5,
# #                         "min_words_vertical": 3, "min_words_horizontal": 3,
# #                         "text_tolerance": 5, "intersection_tolerance": 5
# #                     }
# #                     table_bboxes = [tbl.bbox for tbl in page.find_tables(later_page_table_finder_settings)]
# #                     if not table_bboxes:
# #                         page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)
# #                         if page_text_content is None: page_text_content = ""
# #                     else:
# #                         words_on_page = page.extract_words(keep_blank_chars=False, use_text_flow=True)
# #                         non_table_words = []
# #                         for word in words_on_page:
# #                             word_bbox = (word['x0'], word['top'], word['x1'], word['bottom'])
# #                             is_in_identified_table = False
# #                             for table_bbox in table_bboxes:
# #                                 word_center_x = (word_bbox[0] + word_bbox[2]) / 2
# #                                 word_center_y = (word_bbox[1] + word_bbox[3]) / 2
# #                                 if (table_bbox[0] <= word_center_x <= table_bbox[2] and
# #                                         table_bbox[1] <= word_center_y <= table_bbox[3]):
# #                                     is_in_identified_table = True
# #                                     break
# #                             if not is_in_identified_table:
# #                                 non_table_words.append(word['text'])
# #                         if non_table_words:
# #                             page_text_content = " ".join(non_table_words)
# #                         else:
# #                             page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)
# #                             if page_text_content is None: page_text_content = ""
# #                             page_text_content += "\n[INFO: This page (>{max_pages_for_tables}) was identified as having tables; full text extracted after filtering attempt yielded no words.]\n"
# #
# #                 processed_text_parts.append(
# #                     f"\n--- PAGE {page_number_for_log} ---\n{page_text_content if page_text_content else ''}")
# #
# #         # print("".join(processed_text_parts)) # For debugging
# #         return "".join(processed_text_parts)
# #     except Exception as e:
# #         error_msg = f"Error in preprocess_pdf_text_variant_1_filtered: {type(e).__name__} - {e}"
# #         print(error_msg)
# #         return error_msg
# #
# #
# # def preprocess_pdf_text_variant_2_full_text(pdf_path_or_bytes) -> str:
# #     """
# #     Extracts all text from all pages without any special table handling or filtering,
# #     using layout=True for better readability by the LLM.
# #     """
# #     processed_text_parts = []
# #     try:
# #         with pdfplumber.open(pdf_path_or_bytes) as pdf:
# #             for i, page in enumerate(pdf.pages):
# #                 page_text = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)
# #                 if page_text is None:
# #                     page_text = f"[INFO: Page {i + 1} yielded no text directly]"
# #                 else:
# #                     # Sanitize any accidental "None" strings if extract_text returns it (should not happen with check above)
# #                     page_text = page_text.replace("None", "")
# #                 processed_text_parts.append(f"\n--- PAGE {i + 1} ---\n{page_text}")
# #         # print("Full text extracted for retry.") # For debugging
# #         return "".join(processed_text_parts)
# #     except Exception as e:
# #         error_msg = f"Error in preprocess_pdf_text_variant_2_full_text: {type(e).__name__} - {e}"
# #         print(error_msg)
# #         return error_msg
# #
# #
# # def _call_gemini_api(api_key: str, text_content: str, attempt_description: str, is_retry: bool) -> ParsedDARReport:
# #     """Internal function to call Gemini API and parse response."""
# #     if text_content.startswith("Error in preprocess_pdf_text_"):  # Check for preprocessing errors
# #         return ParsedDARReport(parsing_errors=text_content)
# #
# #     genai.configure(api_key=api_key)
# #     # Using a model known for good instruction following and context handling.
# #     # The user's previous dar_processor.py had 'gemini-2.5-flash-preview-04-17' which might be a preview.
# #     # 'gemini-1.5-flash-latest' is a good generally available option.
# #     model = genai.GenerativeModel('gemini-1.5-flash-latest')
# #
# #     # Base prompt structure matching user's models.py
# #     prompt_text_description = (
# #         "which was extracted using pdfplumber (tables in the first 3 pages are formatted as Markdown, "
# #         "text from later pages has attempted to exclude table content)")
# #     if is_retry:
# #         prompt_text_description = ("which is the FULL text from the PDF, including all tables from all pages, "
# #                                    "as a previous attempt with filtered text was incomplete.")
# #
# #     prompt = f"""
# #     You are an expert GST audit report analyst. Based on the following text from a Departmental Audit Report (DAR),
# #     {prompt_text_description},
# #     extract the specified information and structure it as a JSON object.
# #     For the retry attempt (if indicated), be aware that all text, including all tables, is present.
# #     Focus on narrative sections for audit para details, especially if the text seems dense or tabular in later pages.
# #     Notes like "[INFO: ...]" in the text are for context only and should not be part of the extracted data.
# #
# #     The JSON object should follow this structure precisely:
# #     {{
# #       "header": {{
# #         "audit_group_number": "integer or null (e.g., if 'Group-VI' or 'Gr 6', extract 6; must be between 1 and 30)",
# #         "gstin": "string or null",
# #         "trade_name": "string or null",
# #         "category": "string ('Large', 'Medium', 'Small') or null",
# #         "total_amount_detected_overall_rs": "float or null (numeric value in Rupees)",
# #         "total_amount_recovered_overall_rs": "float or null (numeric value in Rupees)"
# #       }},
# #       "audit_paras": [
# #         {{
# #           "audit_para_number": "integer or null (primary number from para heading, e.g., for 'Para-1...' use 1; must be between 1 and 50)",
# #           "audit_para_heading": "string or null (the descriptive title of the para)",
# #           "revenue_involved_lakhs_rs": "float or null (numeric value in Lakhs of Rupees, e.g., Rs. 50,000 becomes 0.5)",
# #           "revenue_recovered_lakhs_rs": "float or null (numeric value in Lakhs of Rupees)"
# #         }}
# #       ],
# #       "parsing_errors": "string or null (any notes about parsing issues, or if extraction is incomplete)"
# #     }}
# #
# #     Key Instructions:
# #     1.  **Header Information (usually from first 1-3 pages):**
# #         - For `audit_group_number`: Extract the group number as an integer. Example: 'Group-VI' or 'Gr 6' becomes 6. Must be between 1 and 30. If not determinable as such, return null.
# #         - Extract `gstin`, `trade_name`, and `category`.
# #         - `total_amount_detected_overall_rs`: Grand total detection for the entire audit (in Rupees).
# #         - `total_amount_recovered_overall_rs`: Grand total recovery for the entire audit (in Rupees).
# #     2.  **Audit Paras (usually starting after page 3):**
# #         - Identify each distinct audit para. They often start with "Para-X" or similar.
# #         - For `audit_para_number`: Extract the main number from the para heading as an integer (e.g., "Para-1..." or "Para 1." becomes 1). Must be an integer between 1 and 50.
# #         - Extract `audit_para_heading` (the descriptive title/summary of the para).
# #         - Extract "Revenue involved" specific to THAT para and convert to LAKHS of Rupees (amount_in_rs / 100000.0).
# #         - Extract "Revenue recovered" specific to THAT para (e.g., from 'amount paid' or 'party contention') and convert to LAKHS of Rupees.
# #     3.  If any field's value is not found or cannot be determined, use null for that field.
# #     4.  Ensure all monetary values are numbers (float).
# #     5.  `audit_paras` list should contain one object per para. If no paras found, provide an empty list [].
# #
# #     DAR Text Content:
# #     --- START OF DAR TEXT ---
# #     {text_content}
# #     --- END OF DAR TEXT ---
# #
# #     Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
# #     """
# #
# #     print(f"\n--- Calling Gemini: {attempt_description} ---")
# #     # For debugging the prompt sent to Gemini:
# #     # print(f"Prompt for {attempt_description} (first 500 chars):\n{prompt[:500]}...")
# #     # print(f"Prompt for {attempt_description} (last 500 chars):\n...{prompt[-500:]}")
# #
# #     try:
# #         response = model.generate_content(prompt)
# #
# #         cleaned_response_text = response.text.strip()
# #         if cleaned_response_text.startswith("```json"):
# #             cleaned_response_text = cleaned_response_text[7:]
# #         elif cleaned_response_text.startswith("`json"):
# #             cleaned_response_text = cleaned_response_text[6:]
# #         if cleaned_response_text.endswith("```"):
# #             cleaned_response_text = cleaned_response_text[:-3]
# #
# #         if not cleaned_response_text:
# #             error_message = f"Gemini returned an empty response for {attempt_description}."
# #             print(error_message)
# #             return ParsedDARReport(parsing_errors=error_message)
# #
# #         json_data = json.loads(cleaned_response_text)
# #         parsed_report = ParsedDARReport(**json_data)
# #         print(f"Gemini call for {attempt_description} successful. Paras found: {len(parsed_report.audit_paras)}")
# #         return parsed_report
# #     except json.JSONDecodeError as e:
# #         raw_response_text = "No response text available"
# #         if 'response' in locals() and hasattr(response, 'text'):
# #             raw_response_text = response.text
# #         error_message = f"Gemini output ({attempt_description}) was not valid JSON: {e}. Response: '{raw_response_text[:1000]}...'"
# #         print(error_message)
# #         return ParsedDARReport(parsing_errors=error_message)
# #     except Exception as e:
# #         raw_response_text = "No response text available"
# #         if 'response' in locals() and hasattr(response, 'text'):
# #             raw_response_text = response.text
# #         error_message = f"Error ({attempt_description}) during Gemini/Pydantic: {type(e).__name__} - {e}. Response: {raw_response_text[:500]}"
# #         print(error_message)
# #         return ParsedDARReport(parsing_errors=error_message)
# #
# #
# # # This is the main function app.py will call
# # def get_structured_data_with_gemini_orchestrator(api_key: str, pdf_path_or_bytes) -> ParsedDARReport:
# #     """
# #     Orchestrates PDF processing and Gemini calls, with a retry mechanism.
# #     """
# #     # Attempt 1: With filtered text
# #     print("Orchestrator: Attempt 1 - Using preprocessed text with table filtering/formatting...")
# #     text_v1 = preprocess_pdf_text_variant_1_filtered(pdf_path_or_bytes)
# #     # Check if preprocessing itself returned an error string
# #     if text_v1.startswith("Error in preprocess_pdf_text_variant_1_filtered"):
# #         return ParsedDARReport(parsing_errors=text_v1)
# #
# #     report_v1 = _call_gemini_api(api_key, text_v1, "Attempt 1 (Filtered Text)", is_retry=False)
# #
# #     # Define conditions for retry based on audit_paras content
# #     retry_needed = False
# #     if not report_v1.audit_paras:  # No paras found at all
# #         retry_needed = True
# #         print("Orchestrator: Retry Trigger - No audit paras found in first attempt.")
# #     elif report_v1.audit_paras:  # Paras list exists, check for missing headings
# #         # Count paras where heading is None or an empty/whitespace string
# #         paras_with_no_heading = sum(
# #             1 for p in report_v1.audit_paras if not p.audit_para_heading or not p.audit_para_heading.strip())
# #
# #         # Retry if more than 30% of found paras have no heading, AND there's at least one para.
# #         # (Avoid division by zero if len is 0, though covered by the first `if`)
# #         if len(report_v1.audit_paras) > 0 and \
# #                 (paras_with_no_heading / len(report_v1.audit_paras)) >= 0.4:  # 40% threshold for retry
# #             retry_needed = True
# #             print(
# #                 f"Orchestrator: Retry Trigger - {paras_with_no_heading}/{len(report_v1.audit_paras)} paras have missing headings (>=40%).")
# #         elif paras_with_no_heading > 0:
# #             print(
# #                 f"Orchestrator: Note - {paras_with_no_heading}/{len(report_v1.audit_paras)} paras have missing headings, but below retry threshold.")
# #
# #     if retry_needed:
# #         print("\nOrchestrator: Attempt 2 - Using full PDF text without table filtering...")
# #         text_v2 = preprocess_pdf_text_variant_2_full_text(pdf_path_or_bytes)
# #         if text_v2.startswith("Error in preprocess_pdf_text_variant_2_full_text"):
# #             error_msg_v2 = f"Retry preprocessing failed: {text_v2}"
# #             print(f"Orchestrator: {error_msg_v2}")
# #             # Append this error to the first report's errors, if any
# #             if report_v1.parsing_errors:
# #                 report_v1.parsing_errors += f"; {error_msg_v2}"
# #             else:
# #                 report_v1.parsing_errors = error_msg_v2
# #             return report_v1  # Return the (potentially flawed) first attempt
# #
# #         report_v2 = _call_gemini_api(api_key, text_v2, "Attempt 2 (Full Text)", is_retry=True)
# #
# #         # Optionally, decide if report_v2 is "better" than report_v1
# #         # For now, if retry was triggered, we trust the retry result more.
# #         # We could add logic: if report_v2 also has issues, but report_v1 was better, return report_v1.
# #         # Example: if report_v2 has fewer paras or more errors than report_v1 (after a retry was deemed necessary for v1).
# #         # For simplicity, current logic returns report_v2 if retry is done.
# #         if report_v2.parsing_errors and not report_v1.parsing_errors and report_v1.audit_paras:
# #             print(
# #                 "Orchestrator: Retry attempt had parsing errors, but first attempt was clean. Returning first attempt.")
# #             report_v1.parsing_errors = (report_v1.parsing_errors or "") + \
# #                                        f"; Retry also had errors: {report_v2.parsing_errors}"
# #             return report_v1
# #
# #         return report_v2
# #     else:
# #         print("Orchestrator: First attempt deemed sufficient. No retry needed.")
# #         return report_v1
# # # # # dar_processor.py
# # # # import pdfplumber  # Use pdfplumber
# # # # import google.generativeai as genai
# # # # dar_processor.py
# # # import pdfplumber
# # # import google.generativeai as genai
# # # import json
# # # from typing import List, Dict, Any
# # # from models import ParsedDARReport, DARHeaderSchema, AuditParaSchema  # Using the models.py you provided
# # #
# # #
# # # def preprocess_pdf_text(pdf_path_or_bytes, max_pages_for_tables=3) -> str:
# # #     """
# # #     Extracts text from PDF using pdfplumber.
# # #     Formats tables as Markdown for the first `max_pages_for_tables`.
# # #     For subsequent pages, attempts to intelligently filter out only dense tabular data,
# # #     prioritizing preservation of narrative text.
# # #     """
# # #     processed_text_parts = []
# # #     try:
# # #         with pdfplumber.open(pdf_path_or_bytes) as pdf:
# # #             for i, page in enumerate(pdf.pages):
# # #                 page_number_for_log = i + 1
# # #                 page_text_content = ""
# # #
# # #                 if i < max_pages_for_tables:  # For first N pages (0-indexed for pages 1, 2, 3)
# # #                     page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2)
# # #                     if page_text_content is None: page_text_content = ""
# # #
# # #                     # Table extraction settings for initial pages (more liberal to catch tables for markdown)
# # #                     initial_page_table_settings = {
# # #                         "vertical_strategy": "lines",
# # #                         "horizontal_strategy": "lines",
# # #                         "snap_tolerance": 4,
# # #                         "join_tolerance": 4,
# # #                         "min_words_vertical": 2,  # Fairly lenient
# # #                         "min_words_horizontal": 2  # Fairly lenient
# # #                     }
# # #                     tables = page.extract_tables(table_settings=initial_page_table_settings)
# # #
# # #                     if tables:
# # #                         page_text_content += f"\n\n--- Extracted Tables (Page {page_number_for_log}) Start ---\n"
# # #                         for table_idx, table_data in enumerate(tables):
# # #                             if table_data:
# # #                                 page_text_content += f"\n--- Table {table_idx + 1} ---\n"
# # #                                 header_row_data = table_data[0]
# # #                                 if header_row_data:
# # #                                     str_header_row = [str(cell) if cell is not None else "" for cell in header_row_data]
# # #                                     page_text_content += "| " + " | ".join(str_header_row) + " |\n"
# # #                                     page_text_content += "| " + " | ".join(["---"] * len(str_header_row)) + " |\n"
# # #
# # #                                 for row_data in table_data[1:]:
# # #                                     if row_data:
# # #                                         str_row = [str(cell) if cell is not None else "" for cell in row_data]
# # #                                         page_text_content += "| " + " | ".join(str_row) + " |\n"
# # #                         page_text_content += f"--- Extracted Tables (Page {page_number_for_log}) End ---\n\n"
# # #                 else:
# # #                     # For pages after max_pages_for_tables:
# # #                     # Attempt to filter out only clearly identified, dense tables.
# # #
# # #                     # Stricter table finding settings to avoid flagging non-tabular elements
# # #                     later_page_table_finder_settings = {
# # #                         "vertical_strategy": "lines",
# # #                         "horizontal_strategy": "lines",
# # #                         "snap_tolerance": 5,  # Slightly increased tolerance
# # #                         "join_tolerance": 5,  # Slightly increased tolerance
# # #                         "min_words_vertical": 3,  # Require more words to define a table line
# # #                         "min_words_horizontal": 3,  # Require more words to define a table row
# # #                         "text_tolerance": 5,  # Tolerance for aligning text within cells
# # #                         "intersection_tolerance": 5  # Tolerance for cell boundary intersections
# # #                     }
# # #                     table_bboxes = [tbl.bbox for tbl in page.find_tables(later_page_table_finder_settings)]
# # #
# # #                     if not table_bboxes:
# # #                         # If no tables are found with stricter settings, assume the page is mostly narrative.
# # #                         page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2,
# # #                                                               layout=True)  # Use layout for better flow
# # #                         if page_text_content is None: page_text_content = ""
# # #                     else:
# # #                         # If tables ARE found, extract words and filter those clearly inside these tables.
# # #                         words_on_page = page.extract_words(keep_blank_chars=False, use_text_flow=True)
# # #                         non_table_words = []
# # #                         for word in words_on_page:
# # #                             word_bbox = (word['x0'], word['top'], word['x1'], word['bottom'])
# # #                             is_in_identified_table = False
# # #                             for table_bbox in table_bboxes:
# # #                                 # Check if the word's center is within this specific table's bounding box
# # #                                 word_center_x = (word_bbox[0] + word_bbox[2]) / 2
# # #                                 word_center_y = (word_bbox[1] + word_bbox[3]) / 2
# # #                                 if (table_bbox[0] <= word_center_x <= table_bbox[2] and
# # #                                         table_bbox[1] <= word_center_y <= table_bbox[3]):
# # #                                     is_in_identified_table = True
# # #                                     break
# # #                             if not is_in_identified_table:
# # #                                 non_table_words.append(word['text'])
# # #
# # #                         if non_table_words:
# # #                             # Try to reconstruct text flow somewhat from non_table_words
# # #                             # This is a basic reconstruction. For better flow, consider pdfplumber's higher-level text extraction
# # #                             # on a page object that has had table objects notionally "removed" (more complex).
# # #                             page_text_content = " ".join(non_table_words)  # Basic join, might lose some formatting
# # #                         else:
# # #                             # Fallback: If filtering results in no words, but tables were detected,
# # #                             # it suggests the page might be heavily tabular or filtering was too aggressive.
# # #                             # Safest to extract all text with layout=True and let Gemini discern.
# # #                             page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2, layout=True)
# # #                             if page_text_content is None: page_text_content = ""
# # #                             page_text_content += "\n[INFO: This page was identified as having tables; full text extracted for AI review.]\n"
# # #
# # #                 processed_text_parts.append(
# # #                     f"\n--- PAGE {page_number_for_log} ---\n{page_text_content if page_text_content else ''}")
# # #
# # #         # print("".join(processed_text_parts)) # For debugging the preprocessed text
# # #         return "".join(processed_text_parts)
# # #     except Exception as e:
# # #         error_msg = f"Error processing PDF with pdfplumber: {type(e).__name__} - {e}"
# # #         print(error_msg)
# # #         return error_msg
# # #
# # #
# # # def get_structured_data_with_gemini(api_key: str, text_content: str) -> ParsedDARReport:
# # #     if text_content.startswith("Error processing PDF with pdfplumber:"):
# # #         return ParsedDARReport(parsing_errors=text_content)
# # #
# # #     genai.configure(api_key=api_key)
# # #     # Using a model known for good instruction following and context handling.
# # #     # The user's previous dar_processor.py had 'gemini-2.5-flash-preview-04-17'
# # #     # Let's use gemini-1.5-flash-latest as it's generally available and good for this.
# # #     model = genai.GenerativeModel('gemini-1.5-flash-latest')
# # #
# # #     # Prompt needs to align with the user's provided models.py
# # #     prompt = f"""
# # #     You are an expert GST audit report analyst. Based on the following text from a Departmental Audit Report (DAR),
# # #     extract the specified information and structure it as a JSON object.
# # #     Tables from the first 3 pages are formatted in Markdown. For later pages (after page 3),
# # #     text extraction has attempted to prioritize non-tabular narrative content, but some table text might still be present;
# # #     focus on narrative sections for audit para details. Notes like "[INFO: ...]" are for context only.
# # #
# # #     The JSON object should follow this structure precisely:
# # #     {{
# # #       "header": {{
# # #         "audit_group_number": "integer or null (e.g., if 'Group-VI' or 'Gr 6', extract 6; must be between 1 and 30)",
# # #         "gstin": "string or null",
# # #         "trade_name": "string or null",
# # #         "category": "string ('Large', 'Medium', 'Small') or null",
# # #         "total_amount_detected_overall_rs": "float or null (numeric value in Rupees)",
# # #         "total_amount_recovered_overall_rs": "float or null (numeric value in Rupees)"
# # #       }},
# # #       "audit_paras": [
# # #         {{
# # #           "audit_para_number": "integer or null (primary number from para heading, e.g., for 'Para-1...' use 1; must be between 1 and 50)",
# # #           "audit_para_heading": "string or null",
# # #           "revenue_involved_lakhs_rs": "float or null (numeric value in Lakhs of Rupees, e.g., Rs. 50,000 becomes 0.5)",
# # #           "revenue_recovered_lakhs_rs": "float or null (numeric value in Lakhs of Rupees)"
# # #         }}
# # #       ],
# # #       "parsing_errors": "string or null (any notes about parsing issues, or if extraction is incomplete)"
# # #     }}
# # #
# # #     Key Instructions:
# # #     1.  **Header Information (usually from first 1-3 pages):**
# # #         - For `audit_group_number`: Extract the group number as an integer. For example, if the text says 'Group-VI' or 'Gr 6', the value should be 6. It must be an integer between 1 and 30. If you cannot determine an integer matching these criteria, return null.
# # #         - Extract `gstin`, `trade_name`, and `category`.
# # #         - `total_amount_detected_overall_rs`: Grand total detection for the entire audit (in Rupees).
# # #         - `total_amount_recovered_overall_rs`: Grand total recovery for the entire audit (in Rupees).
# # #     2.  **Audit Paras (usually starting after page 3):**
# # #         - Identify each distinct audit para. They often start with "Para-X" or similar.
# # #         - For `audit_para_number`: Extract the main number from the para heading as an integer (e.g., for "Para-1..." or "Para 1.", use 1). It must be an integer between 1 and 50.
# # #         - Extract `audit_para_heading` (the descriptive title of the para).
# # #         - Extract "Revenue involved" specific to THAT para and convert it to LAKHS of Rupees (amount_in_rs / 100000.0).
# # #         - Extract "Revenue recovered" specific to THAT para (e.g. from 'amount paid' or 'party contention') and convert it to LAKHS of Rupees.
# # #     3.  If any field's value is not found or cannot be determined according to the instructions, use null for that field.
# # #     4.  Ensure all monetary values are extracted as numbers (float).
# # #     5.  The 'audit_paras' list should contain one object for each distinct audit para found. If no audit paras are found, provide an empty list [].
# # #
# # #     DAR Text Content:
# # #     --- START OF DAR TEXT ---
# # #     {text_content}
# # #     --- END OF DAR TEXT ---
# # #
# # #     Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
# # #     """
# # #
# # #     try:
# # #         response = model.generate_content(prompt)
# # #
# # #         cleaned_response_text = response.text.strip()
# # #         if cleaned_response_text.startswith("```json"):
# # #             cleaned_response_text = cleaned_response_text[7:]
# # #         elif cleaned_response_text.startswith("`json"):
# # #             cleaned_response_text = cleaned_response_text[6:]
# # #         if cleaned_response_text.endswith("```"):
# # #             cleaned_response_text = cleaned_response_text[:-3]
# # #
# # #         if not cleaned_response_text:
# # #             error_message = "Gemini returned an empty response."
# # #             print(error_message)
# # #             return ParsedDARReport(parsing_errors=error_message)
# # #
# # #         json_data = json.loads(cleaned_response_text)
# # #         parsed_report = ParsedDARReport(**json_data)  # Validation against your models.py
# # #         return parsed_report
# # #     except json.JSONDecodeError as e:
# # #         raw_response_text = "No response text available"
# # #         if 'response' in locals() and hasattr(response, 'text'):
# # #             raw_response_text = response.text
# # #         error_message = f"Gemini output was not valid JSON: {e}. Response text from Gemini: '{raw_response_text[:1000]}...'"
# # #         print(error_message)
# # #         return ParsedDARReport(parsing_errors=error_message)
# # #     except Exception as e:
# # #         raw_response_text = "No response text available"
# # #         if 'response' in locals() and hasattr(response, 'text'):
# # #             raw_response_text = response.text
# # #         error_message = f"Error during Gemini API call or Pydantic validation: {type(e).__name__} - {e}. Gemini response snippet: {raw_response_text[:500]}"
# # #         print(error_message)
# # #         return ParsedDARReport(parsing_errors=error_message)
# # # # import json
# # # # from typing import List, Dict, Any
# # # # from models import ParsedDARReport, DARHeaderSchema, AuditParaSchema  # Pydantic models
# # # #
# # # #
# # # # def preprocess_pdf_text(pdf_path_or_bytes, max_pages_for_tables=3) -> str:
# # # #     """
# # # #     Extracts text from PDF using pdfplumber.
# # # #     Keeps table content formatted as Markdown for the first `max_pages_for_tables`.
# # # #     For subsequent pages, it attempts to extract only non-table text if advanced filtering is enabled.
# # # #     """
# # # #     processed_text_parts = []
# # # #     try:
# # # #         with pdfplumber.open(pdf_path_or_bytes) as pdf:
# # # #             for i, page in enumerate(pdf.pages):
# # # #                 page_text_content = ""
# # # #                 if i < max_pages_for_tables:
# # # #                     # For first N pages, extract all text and explicitly format tables as Markdown
# # # #                     page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2)  # Basic text
# # # #                     if page_text_content is None: page_text_content = ""
# # # #
# # # #                     tables = page.extract_tables(
# # # #                         table_settings={
# # # #                             "vertical_strategy": "lines",
# # # #                             "horizontal_strategy": "lines",
# # # #                             "snap_tolerance": 4,
# # # #                             "join_tolerance": 4,
# # # #                         }
# # # #                     )
# # # #                     if tables:
# # # #                         page_text_content += "\n\n--- Extracted Tables (Page " + str(i + 1) + ") Start ---\n"
# # # #                         for table_data in tables:
# # # #                             if table_data:  # Ensure table is not empty
# # # #                                 # Convert table data to Markdown
# # # #                                 header = "| " + " | ".join(filter(None, map(str, table_data[0]))) + " |"
# # # #                                 separator = "| " + " | ".join(["---"] * len(table_data[0])) + " |"
# # # #                                 body = "\n".join(
# # # #                                     ["| " + " | ".join(filter(None, map(str, row))) + " |" for row in table_data[1:]])
# # # #                                 page_text_content += header + "\n" + separator + "\n" + body + "\n"
# # # #                         page_text_content += "--- Extracted Tables (Page " + str(i + 1) + ") End ---\n\n"
# # # #                 else:
# # # #                     # For subsequent pages, attempt to extract text outside of tables.
# # # #                     # This is an advanced and potentially less reliable part.
# # # #                     # If it causes issues or is too slow, simplify to page.extract_text().
# # # #
# # # #                     # Option 1: Simpler approach - extract all text and rely on Gemini's context understanding
# # # #                     # page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2)
# # # #                     # if page_text_content is None: page_text_content = ""
# # # #
# # # #                     # Option 2: More advanced - try to filter out table text
# # # #                     # (This is where the 'rect_x0' error might have occurred if attributes were misnamed)
# # # #                     words_on_page = page.extract_words(keep_blank_chars=False,
# # # #                                                        use_text_flow=True)  # Standard attributes
# # # #
# # # #                     # Get table bounding boxes
# # # #                     table_bboxes = [tbl.bbox for tbl in page.find_tables(
# # # #                         table_settings={
# # # #                             "vertical_strategy": "lines",
# # # #                             "horizontal_strategy": "lines",
# # # #                             "snap_tolerance": 4,
# # # #                             "join_tolerance": 4,
# # # #                         }
# # # #                     )]
# # # #
# # # #                     non_table_words = []
# # # #                     for word in words_on_page:
# # # #                         # Standard word attributes from pdfplumber are 'x0', 'top', 'x1', 'bottom', 'text'
# # # #                         word_bbox = (word['x0'], word['top'], word['x1'], word['bottom'])
# # # #                         is_in_table = False
# # # #                         for table_bbox in table_bboxes:
# # # #                             # Check if word_bbox is inside or overlaps significantly with table_bbox
# # # #                             # A simple check: if the center of the word is in a table_bbox
# # # #                             word_center_x = (word_bbox[0] + word_bbox[2]) / 2
# # # #                             word_center_y = (word_bbox[1] + word_bbox[3]) / 2
# # # #                             if (table_bbox[0] <= word_center_x <= table_bbox[2] and
# # # #                                     table_bbox[1] <= word_center_y <= table_bbox[3]):
# # # #                                 is_in_table = True
# # # #                                 break
# # # #                         if not is_in_table:
# # # #                             non_table_words.append(word['text'])
# # # #
# # # #                     page_text_content = " ".join(non_table_words)
# # # #                     if not page_text_content.strip() and not table_bboxes:  # If no non-table text and no tables, get all text
# # # #                         page_text_content = page.extract_text(x_tolerance=2, y_tolerance=2)
# # # #                         if page_text_content is None: page_text_content = ""
# # # #
# # # #                 processed_text_parts.append(f"\n--- PAGE {i + 1} ---\n{page_text_content}")
# # # #         print("".join(processed_text_parts))
# # # #         return "".join(processed_text_parts)
# # # #     except Exception as e:
# # # #         print(f"Error processing PDF with pdfplumber: {e}")  # Specific error message
# # # #         # You might want to raise the exception or return a specific error indicator
# # # #         # raise e # Uncomment to see the full traceback in Streamlit if preferred
# # # #         return f"Error processing PDF with pdfplumber: {e}"
# # # #
# # # #
# # # # def get_structured_data_with_gemini(api_key: str, text_content: str) -> ParsedDARReport:
# # # #     if text_content.startswith("Error processing PDF with pdfplumber:"):  # Check if preprocessing failed
# # # #         return ParsedDARReport(parsing_errors=text_content)
# # # #
# # # #     genai.configure(api_key=api_key)
# # # #     model = genai.GenerativeModel('gemini-2.5-flash-preview-04-17')  # Or newer like 'gemini-1.5-flash-latest'
# # # #
# # # #     prompt = f"""
# # # #     You are an expert GST audit report analyst. Based on the following text from a Departmental Audit Report (DAR),
# # # #     which was extracted using pdfplumber (tables in the first 3 pages are formatted as Markdown,
# # # #     text from later pages has attempted to exclude table content),
# # # #     extract the specified information and structure it as a JSON object.
# # # #
# # # #     The JSON object should follow this structure:
# # # #     {{
# # # #       "header": {{
# # # #         "audit_group_number": "integer or null",
# # # #         "gstin": "string or null",
# # # #         "trade_name": "string or null",
# # # #         "category": "string ('Large', 'Medium', 'Small') or null",
# # # #         "total_amount_detected_overall_rs": "float or null (numeric value in Rupees)",
# # # #         "total_amount_recovered_overall_rs": "float or null (numeric value in Rupees)"
# # # #       }},
# # # #       "audit_paras": [
# # # #         {{
# # # #           "audit_para_number": "integer or null",
# # # #           "audit_para_heading": "string or null",
# # # #           "revenue_involved_lakhs_rs": "float or null (numeric value in Lakhs of Rupees, e.g., Rs. 50,000 becomes 0.5)",
# # # #           "revenue_recovered_lakhs_rs": "float or null (numeric value in Lakhs of Rupees)"
# # # #         }}
# # # #       ],
# # # #       "parsing_errors": "string or null (any notes about parsing issues)"
# # # #     }}
# # # #
# # # #     Key Instructions:
# # # #     1.  Header details are usually in the first few pages. Tables from these pages are formatted in Markdown.
# # # #         - if u could not find a integer for "audit_group_number",return null
# # # #         -"total_amount_detected_overall_rs" refers to the grand total detection.
# # # #         - "total_amount_recovered_overall_rs" refers to the grand total recovery.
# # # #     2.  For audit paras (usually after page 3), focus on the narrative text.
# # # #         - Extract para number, heading.
# # # #         - "Revenue involved" specific to that para, converted to LAKHS of Rupees.
# # # #         - "Revenue recovered" specific to that para, converted to LAKHS of Rupees.
# # # #     3.  If a value is not found, use null. Monetary values should be numbers (float).
# # # #     4.  'audit_paras' is a list of objects.
# # # #
# # # #     DAR Text Content:
# # # #     --- START OF DAR TEXT ---
# # # #     {text_content}
# # # #     --- END OF DAR TEXT ---
# # # #
# # # #     Provide ONLY the JSON object as your response.
# # # #     """
# # # #
# # # #     try:
# # # #         response = model.generate_content(prompt)
# # # #
# # # #         cleaned_response_text = response.text.strip()
# # # #         if cleaned_response_text.startswith("```json"):
# # # #             cleaned_response_text = cleaned_response_text[7:]
# # # #         elif cleaned_response_text.startswith("`json"):
# # # #             cleaned_response_text = cleaned_response_text[6:]
# # # #         if cleaned_response_text.endswith("```"):
# # # #             cleaned_response_text = cleaned_response_text[:-3]
# # # #
# # # #         json_data = json.loads(cleaned_response_text)
# # # #         parsed_report = ParsedDARReport(**json_data)
# # # #         return parsed_report
# # # #     except json.JSONDecodeError as e:
# # # #         error_message = f"Gemini output was not valid JSON: {e}. Response text from Gemini: '{response.text[:1000]}...'"
# # # #         print(error_message)
# # # #         return ParsedDARReport(parsing_errors=error_message)
# # # #     except Exception as e:
# # # #         error_message = f"Error during Gemini API call or Pydantic validation: {type(e).__name__} - {e}"
# # # #         print(error_message)
# # # #         if 'response' in locals() and hasattr(response, 'text'):
# # # #             error_message += f" Gemini response snippet: {response.text[:200]}"
# # # #         return ParsedDARReport(parsing_errors=error_message)
