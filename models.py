# models.py
from pydantic import BaseModel, Field
from typing import List, Optional

class AuditParaSchema(BaseModel):
    audit_para_number: Optional[int] = Field(None, description="The number of the audit para.it can take only integers 1 to 50, e.g., '1', '2'")
    audit_para_heading: Optional[str] = Field(None, description="The heading or title of the audit para.")
    revenue_involved_lakhs_rs: Optional[float] = Field(None, description="Revenue involved in this specific audit para, converted to Lakhs Rs.")
    revenue_recovered_lakhs_rs: Optional[float] = Field(None, description="Revenue recovered for this specific audit para, converted to Lakhs Rs.")
    status_of_para: Optional[str] = Field(None, description="Status of the para, e.g., 'Agreed and Paid', 'Agreed yet to pay', 'Partially agreed and paid', 'Partially agreed, yet to paid', 'Not agreed'")

class DARHeaderSchema(BaseModel):
    audit_group_number: Optional[int] = Field(None, description="Audit Group Number in integer ( 1 to 30), if given in roman eg.'Group-VI' convert as '6'")
    gstin: Optional[str] = Field(None, description="GSTIN of the taxpayer, e.g., '27AAAFP6015CIZQ'")
    trade_name: Optional[str] = Field(None, description="Name of the taxpayer", example="M/s. Taxpayer Name")
    category: Optional[str] = Field(None, description="Category of the taxpayer, e.g., 'Medium', 'Large', 'Small'")
    total_amount_detected_overall_rs: Optional[float] = Field(None, description="Overall total amount detected in the DAR (in Rs, not Lakhs).")
    total_amount_recovered_overall_rs: Optional[float] = Field(None, description="Overall total amount recovered in the DAR (in Rs, not Lakhs).")

class ParsedDARReport(BaseModel):
    header: Optional[DARHeaderSchema] = None
    audit_paras: List[AuditParaSchema] = []
    parsing_errors: Optional[str] = Field(None, description="Any errors or notes from the parsing process.")

# For the final flattened table output
class FlattenedAuditData(BaseModel):
    audit_group_number: Optional[int] = None
    gstin: Optional[str] = None
    trade_name: Optional[str] = None
    category: Optional[str] = None
    total_amount_detected_overall_rs: Optional[float] = None # Overall
    total_amount_recovered_overall_rs: Optional[float] = None # Overall
    audit_para_number: Optional[int] = None
    audit_para_heading: Optional[str] = None
    revenue_involved_lakhs_rs: Optional[float] = None # Per para
    revenue_recovered_lakhs_rs: Optional[float] = None # Per para
    status_of_para: Optional[str] = None # Per para
    # audit_circle_number will be handled during sheet append# from pydantic import BaseModel, Field
# from typing import List, Optional

# class AuditParaSchema(BaseModel):
#     audit_para_number: Optional[int] = Field(None, description="The number of the audit para.it can take only integers 1 to 50, e.g., '1', '2'")
#     audit_para_heading: Optional[str] = Field(None, description="The heading or title of the audit para.")
#     #date_of_audit_plan: Optional[str] = Field(None,

#     #latest_date_of_visit: Optional[str] = Field(None,

#     revenue_involved_lakhs_rs: Optional[float] = Field(None, description="Revenue involved in this specific audit para, converted to Lakhs Rs.")
#     revenue_recovered_lakhs_rs: Optional[float] = Field(None, description="Revenue recovered for this specific audit para, converted to Lakhs Rs.")

# class DARHeaderSchema(BaseModel):
#     audit_group_number: Optional[int] = Field(None, description="Audit Group Number in integer ( 1 to 30), if given in roman eg.'Group-VI' convert as '6'")
#     #audit_circle_number:Optional[int]=Field(None, description="Audit Circle Number in integer ( 1 to 10), if u cant find audit circle from text , derive from audit group number . the formula is 30 audit groups are divided into 10 circles ie 3 audit groups per cirlce. Audit group 1,2,3 are circle no.1 audit group  4,5,6 are cirlce No.2 and so on")
#     gstin: Optional[str] = Field(None, description="GSTIN of the taxpayer, e.g., '27AAAFP6015CIZQ'")
#     trade_name: Optional[str] = Field(None, description="Name of the taxpayer", example="M/s. Taxpayer Name") # Fixed: Use example keyword
#     category: Optional[str] = Field(None, description="Category of the taxpayer, e.g., 'Medium', 'Large', 'Small'")
#     total_amount_detected_overall_rs: Optional[float] = Field(None, description="Overall total amount detected in the DAR (in Rs, not Lakhs).")
#     total_amount_recovered_overall_rs: Optional[float] = Field(None, description="Overall total amount recovered in the DAR (in Rs, not Lakhs).")

# class ParsedDARReport(BaseModel):
#     header: Optional[DARHeaderSchema] = None
#     audit_paras: List[AuditParaSchema] = []
#     parsing_errors: Optional[str] = Field(None, description="Any errors or notes from the parsing process.")

# # For the final flattened table output
# class FlattenedAuditData(BaseModel):
#     audit_group_number: Optional[int] = None
#     #audit_circle_number: Optional[int] = None
#     #date_of_audit_plan: Optional[str] = None
#     #latest_date_of_visit: Optional[str] = None
#     gstin: Optional[str] = None
#     trade_name: Optional[str] = None
#     category: Optional[str] = None
#     total_amount_detected_overall_rs: Optional[float] = None # Overall
#     total_amount_recovered_overall_rs: Optional[float] = None # Overall
#     audit_para_number: Optional[int] = None
#     audit_para_heading: Optional[str] = None
#     revenue_involved_lakhs_rs: Optional[float] = None # Per para
#     revenue_recovered_lakhs_rs: Optional[float] = None # Per para
