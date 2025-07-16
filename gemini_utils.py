# gemini_utils.py - Enhanced with debugging and error handling
import json
import time
import datetime
import sys
import traceback
from typing import Optional
from models import ParsedDARReport

def debug_print(message, level="INFO"):
    """Print debug messages with timestamp"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [GEMINI-{level}] {message}")
    sys.stdout.flush()

def debug_exception(e, context=""):
    """Print detailed exception information"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [GEMINI-ERROR] {context}")
    print(f"Exception type: {type(e).__name__}")
    print(f"Exception message: {str(e)}")
    print("Traceback:")
    traceback.print_exc()
    sys.stdout.flush()

def get_structured_data_with_gemini(api_key: str, text_content: str, max_retries=2) -> ParsedDARReport:
    debug_print(f"Starting Gemini processing with API key: {api_key[:10]}... (max_retries: {max_retries})")
    
    # Validate API key
    if not api_key or api_key == "YOUR_API_KEY_HERE" or api_key == "YOUR_API_KEY_HERE_FALLBACK":
        debug_print("ERROR: Invalid or missing API key")
        return ParsedDARReport(parsing_errors="Gemini API Key not configured.")
    
    # Check for PDF preprocessing errors
    if text_content.startswith("Error processing PDF with pdfplumber:") or \
            text_content.startswith("Error in preprocess_pdf_text_"):
        debug_print(f"ERROR: PDF preprocessing failed - {text_content[:100]}...")
        return ParsedDARReport(parsing_errors=text_content)

    # Initialize Gemini
    try:
        debug_print("Importing google.generativeai")
        import google.generativeai as genai
        
        debug_print("Configuring Gemini API")
        genai.configure(api_key=api_key)
        
        debug_print("Creating model instance (gemini-1.5-flash)")
        # Use gemini-1.5-flash for free tier (better than gemini-1.5-flash-latest)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
    except ImportError as e:
        debug_exception(e, "Failed to import google.generativeai")
        return ParsedDARReport(parsing_errors=f"Failed to import Gemini library: {str(e)}")
    except Exception as e:
        debug_exception(e, "Failed to initialize Gemini")
        return ParsedDARReport(parsing_errors=f"Failed to initialize Gemini: {str(e)}")

    # Validate text content
    if not text_content or len(text_content.strip()) < 50:
        debug_print(f"ERROR: Text content too short - length: {len(text_content) if text_content else 0}")
        return ParsedDARReport(parsing_errors="Text content too short or empty for analysis.")

    # Don't truncate - send full text to Gemini
    # Gemini 1.5 Flash can handle much larger inputs (up to 1M tokens)
    original_length = len(text_content)
    debug_print(f"Text length: {original_length} characters - sending full content to Gemini")
    
    # For very large texts, we might need to handle them differently, but let's try full text first
    if original_length > 100000:  # 100K chars
        debug_print(f"WARNING: Very large text ({original_length} chars). If this fails, consider chunking.")
        # But still send the full text - don't truncate

    # Prepare prompt
    prompt = f"""
    You are an expert GST audit report analyst. Based on the following text from a Departmental Audit Report (DAR),
    extract the specified information and structure it as a JSON object.

    The JSON object should follow this structure precisely:
    {{
      "header": {{
        "audit_group_number": null,
        "gstin": "string or null",
        "trade_name": "string or null",
        "category": "string or null (Large/Medium/Small)",
        "total_amount_detected_overall_rs": "float or null",
        "total_amount_recovered_overall_rs": "float or null"
      }},
      "audit_paras": [
        {{
          "audit_para_number": "integer or null (1-50)",
          "audit_para_heading": "string or null",
          "revenue_involved_lakhs_rs": "float or null (in Lakhs)",
          "revenue_recovered_lakhs_rs": "float or null (in Lakhs)",
          "status_of_para": "string or null (Agreed and Paid/Agreed yet to pay/Partially agreed and paid/Partially agreed, yet to paid/Not agreed)"
        }}
      ],
      "parsing_errors": "string or null"
    }}

    Instructions:
    1. Extract trade_name, gstin, category from the document
    2. Find audit paras with numbers, headings, and amounts
    3. Convert amounts to appropriate units (Lakhs for para amounts)
    4. Use null for missing values
    5. If extraction fails, note in parsing_errors

    DAR Text:
    {text_content}

    Respond with ONLY the JSON object, no explanations.
    """

    debug_print(f"Prompt prepared - length: {len(prompt)} characters")

    attempt = 0
    last_exception = None
    
    while attempt <= max_retries:
        attempt += 1
        debug_print(f"Attempt {attempt}/{max_retries + 1}")
        
        try:
            # Enhanced generation config for larger texts
            debug_print("Sending request to Gemini API")
            
            generation_config = genai.types.GenerationConfig(
                candidate_count=1,
                temperature=0.1,
                max_output_tokens=8192,  # Increased for larger responses
                # Removed stop_sequences to allow full processing
            )
            
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
            ]
            
            debug_print(f"Generation config: temperature=0.1, max_tokens=8192")
            
            response = model.generate_content(
                prompt,
                generation_config=generation_config,
                safety_settings=safety_settings
            )
            
            debug_print(f"Response received: {bool(response)}")
            
            if not response:
                error_message = f"Gemini returned no response on attempt {attempt}. This might be due to free tier rate limits."
                debug_print(f"ERROR: {error_message}")
                last_exception = ValueError(error_message)
                if attempt > max_retries:
                    return ParsedDARReport(parsing_errors=error_message)
                # Longer wait for free tier
                wait_time = 5 + (attempt * 2)
                debug_print(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
                continue
            
            if not response.text:
                error_message = f"Gemini returned empty response.text on attempt {attempt}. This might be due to free tier rate limits."
                debug_print(f"ERROR: {error_message}")
                last_exception = ValueError(error_message)
                if attempt > max_retries:
                    return ParsedDARReport(parsing_errors=error_message)
                # Longer wait for free tier
                wait_time = 5 + (attempt * 2)
                debug_print(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
                continue
            
            debug_print(f"Response text received - length: {len(response.text)} characters")
            debug_print(f"Response text preview: {response.text[:200]}...")
            
            cleaned_response_text = response.text.strip()
            
            # Clean up common markdown formatting
            debug_print("Cleaning response text")
            original_cleaned = cleaned_response_text
            
            if cleaned_response_text.startswith("```json"):
                cleaned_response_text = cleaned_response_text[7:]
                debug_print("Removed ```json prefix")
            elif cleaned_response_text.startswith("```"):
                cleaned_response_text = cleaned_response_text[3:]
                debug_print("Removed ``` prefix")
            elif cleaned_response_text.startswith("`json"):
                cleaned_response_text = cleaned_response_text[5:]
                debug_print("Removed `json prefix")
            elif cleaned_response_text.startswith("`"):
                cleaned_response_text = cleaned_response_text[1:]
                debug_print("Removed ` prefix")
            
            if cleaned_response_text.endswith("```"):
                cleaned_response_text = cleaned_response_text[:-3]
                debug_print("Removed ``` suffix")
            elif cleaned_response_text.endswith("`"):
                cleaned_response_text = cleaned_response_text[:-1]
                debug_print("Removed ` suffix")

            if not cleaned_response_text:
                error_message = f"Gemini response was empty after cleaning on attempt {attempt}."
                debug_print(f"ERROR: {error_message}")
                debug_print(f"Original response: {original_cleaned}")
                last_exception = ValueError(error_message)
                if attempt > max_retries:
                    return ParsedDARReport(parsing_errors=error_message)
                wait_time = 5 + attempt
                debug_print(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
                continue

            debug_print(f"Cleaned response text - length: {len(cleaned_response_text)} characters")
            debug_print(f"Cleaned response preview: {cleaned_response_text[:200]}...")

            # Try to parse JSON
            try:
                debug_print("Attempting to parse JSON")
                json_data = json.loads(cleaned_response_text)
                debug_print("JSON parsed successfully")
                debug_print(f"JSON keys: {list(json_data.keys()) if isinstance(json_data, dict) else 'Not a dict'}")
                
            except json.JSONDecodeError as json_e:
                error_message = f"Invalid JSON from Gemini (Attempt {attempt}): {str(json_e)[:200]}..."
                debug_print(f"ERROR: {error_message}")
                debug_print(f"Problematic JSON text: {cleaned_response_text[:500]}...")
                last_exception = json_e
                if attempt > max_retries:
                    return ParsedDARReport(parsing_errors=error_message)
                wait_time = 5 + (attempt * 2)
                debug_print(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
                continue
            
            # Validate JSON structure
            debug_print("Validating JSON structure")
            if "header" not in json_data or "audit_paras" not in json_data:
                error_message = f"Gemini response missing required keys (Attempt {attempt}). Using fallback structure."
                debug_print(f"WARNING: {error_message}")
                debug_print(f"Available keys: {list(json_data.keys()) if isinstance(json_data, dict) else 'Not a dict'}")
                
                # Create fallback structure
                if "header" not in json_data:
                    json_data["header"] = {}
                    debug_print("Added empty header")
                if "audit_paras" not in json_data:
                    json_data["audit_paras"] = []
                    debug_print("Added empty audit_paras")
                json_data["parsing_errors"] = error_message

            # Try to create ParsedDARReport
            try:
                debug_print("Creating ParsedDARReport from JSON data")
                parsed_report = ParsedDARReport(**json_data)
                debug_print("ParsedDARReport created successfully")
                
                # Log some details about the parsed report
                debug_print(f"Header present: {parsed_report.header is not None}")
                debug_print(f"Number of audit paras: {len(parsed_report.audit_paras)}")
                if parsed_report.header:
                    debug_print(f"Trade name: {parsed_report.header.trade_name}")
                    debug_print(f"GSTIN: {parsed_report.header.gstin}")
                
                return parsed_report
                
            except Exception as pydantic_e:
                error_message = f"Data validation error (Attempt {attempt}): {str(pydantic_e)[:200]}..."
                debug_exception(pydantic_e, f"Pydantic validation failed on attempt {attempt}")
                debug_print(f"JSON data that failed validation: {json_data}")
                last_exception = pydantic_e
                if attempt > max_retries:
                    return ParsedDARReport(parsing_errors=error_message)
                wait_time = 5 + (attempt * 2)
                debug_print(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
                continue
                
        except Exception as e:
            error_message = f"API Error (Attempt {attempt}): {type(e).__name__} - {str(e)[:200]}..."
            debug_exception(e, f"General API error on attempt {attempt}")
            
            # Handle specific free tier errors
            if "quota" in str(e).lower() or "rate" in str(e).lower():
                error_message = f"Free tier quota/rate limit exceeded (Attempt {attempt}). Please wait and try again."
                debug_print(f"QUOTA ERROR: {error_message}")
                last_exception = e
                if attempt > max_retries:
                    return ParsedDARReport(parsing_errors=error_message)
                # Longer wait for quota issues
                wait_time = 30
                debug_print(f"Waiting {wait_time} seconds for quota reset")
                time.sleep(wait_time)
                continue
                
            elif "billing" in str(e).lower():
                error_message = "Billing issue detected. For free tier, ensure you have a valid Google account and the API key is generated correctly."
                debug_print(f"BILLING ERROR: {error_message}")
                return ParsedDARReport(parsing_errors=error_message)
                
            elif "API_KEY" in str(e).upper() or "auth" in str(e).lower():
                error_message = "Authentication error. Please check your API key is valid and generated from Google AI Studio."
                debug_print(f"AUTH ERROR: {error_message}")
                return ParsedDARReport(parsing_errors=error_message)
                
            elif "RESOURCE_EXHAUSTED" in str(e).upper():
                error_message = f"Resource exhausted (Attempt {attempt}). Free tier limits reached. Please wait and try again."
                debug_print(f"RESOURCE ERROR: {error_message}")
                last_exception = e
                if attempt > max_retries:
                    return ParsedDARReport(parsing_errors=error_message)
                wait_time = 60  # Longer wait for resource exhaustion
                debug_print(f"Waiting {wait_time} seconds for resource reset")
                time.sleep(wait_time)
                continue
            
            last_exception = e
            if attempt > max_retries:
                return ParsedDARReport(parsing_errors=error_message)
            wait_time = 5 + (attempt * 2)
            debug_print(f"Waiting {wait_time} seconds before retry")
            time.sleep(wait_time)
    
    # If we get here, all attempts failed
    final_error = f"Gemini API failed after {max_retries + 1} attempts. Last error: {str(last_exception)[:200]}... Try again in a few minutes (free tier rate limits)."
    debug_print(f"FINAL ERROR: {final_error}")
    return ParsedDARReport(parsing_errors=final_error)
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

#     try:
#         import google.generativeai as genai
#         genai.configure(api_key=api_key)
        
#         # Use gemini-1.5-flash for free tier (better than gemini-1.5-flash-latest)
#         model = genai.GenerativeModel('gemini-1.5-flash')
#     except Exception as e:
#         return ParsedDARReport(parsing_errors=f"Failed to initialize Gemini: {str(e)}")

#     # Check if text is too short or empty
#     if not text_content or len(text_content.strip()) < 50:
#         return ParsedDARReport(parsing_errors="Text content too short or empty for analysis.")

#     # Free tier has input limits, so truncate if too long
#     # Free tier limit is around 30K characters for gemini-1.5-flash
#     MAX_FREE_TIER_CHARS = 25000  # Leave some buffer
#     if len(text_content) > MAX_FREE_TIER_CHARS:
#         text_content = text_content[:MAX_FREE_TIER_CHARS] + "\n[INFO: Text truncated due to free tier limits]"

#     prompt = f"""
#     You are an expert GST audit report analyst. Based on the following text from a Departmental Audit Report (DAR),
#     extract the specified information and structure it as a JSON object.

#     The JSON object should follow this structure precisely:
#     {{
#       "header": {{
#         "audit_group_number": null,
#         "gstin": "string or null",
#         "trade_name": "string or null",
#         "category": "string or null (Large/Medium/Small)",
#         "total_amount_detected_overall_rs": "float or null",
#         "total_amount_recovered_overall_rs": "float or null"
#       }},
#       "audit_paras": [
#         {{
#           "audit_para_number": "integer or null (1-50)",
#           "audit_para_heading": "string or null",
#           "revenue_involved_lakhs_rs": "float or null (in Lakhs)",
#           "revenue_recovered_lakhs_rs": "float or null (in Lakhs)",
#           "status_of_para": "string or null (Agreed and Paid/Agreed yet to pay/Partially agreed and paid/Partially agreed, yet to paid/Not agreed)"
#         }}
#       ],
#       "parsing_errors": "string or null"
#     }}

#     Instructions:
#     1. Extract trade_name, gstin, category from the document
#     2. Find audit paras with numbers, headings, and amounts
#     3. Convert amounts to appropriate units (Lakhs for para amounts)
#     4. Use null for missing values
#     5. If extraction fails, note in parsing_errors

#     DAR Text:
#     {text_content}

#     Respond with ONLY the JSON object, no explanations.
#     """

#     attempt = 0
#     last_exception = None
    
#     while attempt <= max_retries:
#         attempt += 1
#         try:
#             # Free tier friendly generation config
#             response = model.generate_content(
#                 prompt,
#                 generation_config=genai.types.GenerationConfig(
#                     candidate_count=1,
#                     temperature=0.1,
#                     max_output_tokens=4096,  # Reduced for free tier
#                     stop_sequences=["\n\n---", "END_JSON"]
#                 ),
#                 safety_settings=[
#                     {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
#                     {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
#                     {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
#                     {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
#                 ]
#             )
            
#             if not response or not response.text:
#                 error_message = f"Gemini returned empty response on attempt {attempt}. This might be due to free tier rate limits."
#                 last_exception = ValueError(error_message)
#                 if attempt > max_retries:
#                     return ParsedDARReport(parsing_errors=error_message)
#                 # Longer wait for free tier
#                 time.sleep(5 + (attempt * 2))
#                 continue
            
#             cleaned_response_text = response.text.strip()
            
#             # Clean up common markdown formatting
#             if cleaned_response_text.startswith("```json"):
#                 cleaned_response_text = cleaned_response_text[7:]
#             elif cleaned_response_text.startswith("```"):
#                 cleaned_response_text = cleaned_response_text[3:]
#             elif cleaned_response_text.startswith("`json"):
#                 cleaned_response_text = cleaned_response_text[5:]
#             elif cleaned_response_text.startswith("`"):
#                 cleaned_response_text = cleaned_response_text[1:]
            
#             if cleaned_response_text.endswith("```"):
#                 cleaned_response_text = cleaned_response_text[:-3]
#             elif cleaned_response_text.endswith("`"):
#                 cleaned_response_text = cleaned_response_text[:-1]

#             if not cleaned_response_text:
#                 error_message = f"Gemini response was empty after cleaning on attempt {attempt}."
#                 last_exception = ValueError(error_message)
#                 if attempt > max_retries:
#                     return ParsedDARReport(parsing_errors=error_message)
#                 time.sleep(5 + attempt)
#                 continue

#             # Try to parse JSON
#             try:
#                 json_data = json.loads(cleaned_response_text)
#             except json.JSONDecodeError as json_e:
#                 error_message = f"Invalid JSON from Gemini (Attempt {attempt}): {str(json_e)[:200]}..."
#                 last_exception = json_e
#                 if attempt > max_retries:
#                     return ParsedDARReport(parsing_errors=error_message)
#                 time.sleep(5 + (attempt * 2))
#                 continue
            
#             # Validate JSON structure
#             if "header" not in json_data or "audit_paras" not in json_data:
#                 error_message = f"Gemini response missing required keys (Attempt {attempt}). Using fallback structure."
#                 # Create fallback structure
#                 if "header" not in json_data:
#                     json_data["header"] = {}
#                 if "audit_paras" not in json_data:
#                     json_data["audit_paras"] = []
#                 json_data["parsing_errors"] = error_message

#             # Try to create ParsedDARReport
#             try:
#                 parsed_report = ParsedDARReport(**json_data)
#                 return parsed_report
#             except Exception as pydantic_e:
#                 error_message = f"Data validation error (Attempt {attempt}): {str(pydantic_e)[:200]}..."
#                 last_exception = pydantic_e
#                 if attempt > max_retries:
#                     return ParsedDARReport(parsing_errors=error_message)
#                 time.sleep(5 + (attempt * 2))
#                 continue
                
#         except Exception as e:
#             error_message = f"API Error (Attempt {attempt}): {type(e).__name__} - {str(e)[:200]}..."
            
#             # Handle specific free tier errors
#             if "quota" in str(e).lower() or "rate" in str(e).lower():
#                 error_message = f"Free tier quota/rate limit exceeded (Attempt {attempt}). Please wait and try again."
#                 last_exception = e
#                 if attempt > max_retries:
#                     return ParsedDARReport(parsing_errors=error_message)
#                 # Longer wait for quota issues
#                 time.sleep(30)
#                 continue
#             elif "billing" in str(e).lower():
#                 return ParsedDARReport(parsing_errors="Billing issue detected. For free tier, ensure you have a valid Google account and the API key is generated correctly.")
            
#             last_exception = e
#             if attempt > max_retries:
#                 return ParsedDARReport(parsing_errors=error_message)
#             time.sleep(5 + (attempt * 2))
    
#     # If we get here, all attempts failed
#     return ParsedDARReport(
#         parsing_errors=f"Gemini API failed after {max_retries + 1} attempts. Last error: {str(last_exception)[:200]}... Try again in a few minutes (free tier rate limits)."
#     )
# # def get_structured_data_with_gemini(api_key: str, text_content: str, max_retries=2) -> ParsedDARReport:
# #     if not api_key or api_key == "YOUR_API_KEY_HERE":
# #         return ParsedDARReport(parsing_errors="Gemini API Key not configured.")
# #     if text_content.startswith("Error processing PDF with pdfplumber:") or \
# #             text_content.startswith("Error in preprocess_pdf_text_"):
# #         return ParsedDARReport(parsing_errors=text_content)

# #     genai.configure(api_key=api_key)
# #     model = genai.GenerativeModel('gemini-1.5-flash-latest')

# #     prompt = f"""
# #     You are an expert GST audit report analyst. Based on the following FULL text from a Departmental Audit Report (DAR),
# #     where all text from all pages, including tables, is provided, extract the specified information
# #     and structure it as a JSON object. Focus on identifying narrative sections for audit para details,
# #     even if they are intermingled with tabular data. Notes like "[INFO: ...]" in the text are for context only.

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
# #           "revenue_recovered_lakhs_rs": "float or null (numeric value in Lakhs of Rupees)",
# #           "status_of_para": "string or null (Possible values: 'Agreed and Paid', 'Agreed yet to pay', 'Partially agreed and paid', 'Partially agreed, yet to paid', 'Not agreed')"
# #         }}
# #       ],
# #       "parsing_errors": "string or null (any notes about parsing issues, or if extraction is incomplete)"
# #     }}

# #     Key Instructions:
# #     1.  Header Information: Extract `audit_group_number` (as integer 1-30, e.g., 'Group-VI' becomes 6), `gstin`, `trade_name`, `category`, `total_amount_detected_overall_rs`, `total_amount_recovered_overall_rs`.
# #     2.  Audit Paras: Identify each distinct para. Extract `audit_para_number` (as integer 1-50), `audit_para_heading`, `revenue_involved_lakhs_rs` (converted to Lakhs), `revenue_recovered_lakhs_rs` (converted to Lakhs), and `status_of_para`.
# #     3.  For `status_of_para`, strictly choose from: 'Agreed and Paid', 'Agreed yet to pay', 'Partially agreed and paid', 'Partially agreed, yet to paid', 'Not agreed'. If the status is unclear or different, use null.
# #     4.  Use null for missing values. Monetary values as float.
# #     5.  If no audit paras found, `audit_paras` should be an empty list [].

# #     DAR Text Content:
# #     --- START OF DAR TEXT ---
# #     {text_content}
# #     --- END OF DAR TEXT ---

# #     Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
# #     """

# #     attempt = 0
# #     last_exception = None
# #     while attempt <= max_retries:
# #         attempt += 1
# #         try:
# #             response = model.generate_content(prompt)
# #             cleaned_response_text = response.text.strip()
# #             if cleaned_response_text.startswith("```json"):
# #                 cleaned_response_text = cleaned_response_text[7:]
# #             elif cleaned_response_text.startswith("`json"):
# #                 cleaned_response_text = cleaned_response_text[6:]
# #             if cleaned_response_text.endswith("```"): cleaned_response_text = cleaned_response_text[:-3]

# #             if not cleaned_response_text:
# #                 error_message = f"Gemini returned an empty response on attempt {attempt}."
# #                 last_exception = ValueError(error_message)
# #                 if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #                 time.sleep(1 + attempt);
# #                 continue

# #             json_data = json.loads(cleaned_response_text)
# #             if "header" not in json_data or "audit_paras" not in json_data:
# #                 error_message = f"Gemini response (Attempt {attempt}) missing 'header' or 'audit_paras' key. Response: {cleaned_response_text[:500]}"
# #                 last_exception = ValueError(error_message)
# #                 if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #                 time.sleep(1 + attempt);
# #                 continue

# #             parsed_report = ParsedDARReport(**json_data)
# #             return parsed_report
# #         except json.JSONDecodeError as e:
# #             raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
# #             error_message = f"Gemini output (Attempt {attempt}) was not valid JSON: {e}. Response: '{raw_response_text[:1000]}...'"
# #             last_exception = e
# #             if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #             time.sleep(attempt * 2)
# #         except Exception as e:
# #             raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
# #             error_message = f"Error (Attempt {attempt}) during Gemini/Pydantic: {type(e).__name__} - {e}. Response: {raw_response_text[:500]}"
# #             last_exception = e
# #             if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #             time.sleep(attempt * 2)
# #     return ParsedDARReport(
# #         parsing_errors=f"Gemini call failed after {max_retries + 1} attempts. Last error: {last_exception}")
# #     # # gemini_utils.py
# # import streamlit as st
# # import json
# # import time
# # import google.generativeai as genai
# # from models import ParsedDARReport # Ensure models.py is in the same directory or installable

# # def get_structured_data_with_gemini(api_key: str, text_content: str, max_retries=2) -> ParsedDARReport:
# #     if not api_key or api_key == "YOUR_API_KEY_HERE":
# #         return ParsedDARReport(parsing_errors="Gemini API Key not configured.")
# #     if text_content.startswith("Error processing PDF with pdfplumber:") or \
# #             text_content.startswith("Error in preprocess_pdf_text_"):
# #         return ParsedDARReport(parsing_errors=text_content)

# #     genai.configure(api_key=api_key)
# #     model = genai.GenerativeModel('gemini-1.5-flash-latest')

# #     prompt = f"""
# #     You are an expert GST audit report analyst. Based on the following FULL text from a Departmental Audit Report (DAR),
# #     where all text from all pages, including tables, is provided, extract the specified information
# #     and structure it as a JSON object. Focus on identifying narrative sections for audit para details,
# #     even if they are intermingled with tabular data. Notes like "[INFO: ...]" in the text are for context only.

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

# #     Key Instructions:
# #     1.  Header Information: Extract `audit_group_number` (as integer 1-30, e.g., 'Group-VI' becomes 6), `gstin`, `trade_name`, `category`, `total_amount_detected_overall_rs`, `total_amount_recovered_overall_rs`.
# #     2.  Audit Paras: Identify each distinct para. Extract `audit_para_number` (as integer 1-50), `audit_para_heading`, `revenue_involved_lakhs_rs` (converted to Lakhs), `revenue_recovered_lakhs_rs` (converted to Lakhs).
# #     3.  Use null for missing values. Monetary values as float.
# #     4.  If no audit paras found, `audit_paras` should be an empty list [].

# #     DAR Text Content:
# #     --- START OF DAR TEXT ---
# #     {text_content}
# #     --- END OF DAR TEXT ---

# #     Provide ONLY the JSON object as your response. Do not include any explanatory text before or after the JSON.
# #     """

# #     attempt = 0
# #     last_exception = None
# #     while attempt <= max_retries:
# #         attempt += 1
# #         try:
# #             response = model.generate_content(prompt)
# #             cleaned_response_text = response.text.strip()
# #             if cleaned_response_text.startswith("```json"):
# #                 cleaned_response_text = cleaned_response_text[7:]
# #             elif cleaned_response_text.startswith("`json"):
# #                 cleaned_response_text = cleaned_response_text[6:]
# #             if cleaned_response_text.endswith("```"): cleaned_response_text = cleaned_response_text[:-3]

# #             if not cleaned_response_text:
# #                 error_message = f"Gemini returned an empty response on attempt {attempt}."
# #                 last_exception = ValueError(error_message)
# #                 if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #                 time.sleep(1 + attempt);
# #                 continue

# #             json_data = json.loads(cleaned_response_text)
# #             if "header" not in json_data or "audit_paras" not in json_data:
# #                 error_message = f"Gemini response (Attempt {attempt}) missing 'header' or 'audit_paras' key. Response: {cleaned_response_text[:500]}"
# #                 last_exception = ValueError(error_message)
# #                 if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #                 time.sleep(1 + attempt);
# #                 continue

# #             parsed_report = ParsedDARReport(**json_data)
# #             return parsed_report
# #         except json.JSONDecodeError as e:
# #             raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
# #             error_message = f"Gemini output (Attempt {attempt}) was not valid JSON: {e}. Response: '{raw_response_text[:1000]}...'"
# #             last_exception = e
# #             if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #             time.sleep(attempt * 2)
# #         except Exception as e:
# #             raw_response_text = locals().get('response', {}).text if 'response' in locals() else "No response text captured"
# #             error_message = f"Error (Attempt {attempt}) during Gemini/Pydantic: {type(e).__name__} - {e}. Response: {raw_response_text[:500]}"
# #             last_exception = e
# #             if attempt > max_retries: return ParsedDARReport(parsing_errors=error_message)
# #             time.sleep(attempt * 2)
# #     return ParsedDARReport(
# #         parsing_errors=f"Gemini call failed after {max_retries + 1} attempts. Last error: {last_exception}")
