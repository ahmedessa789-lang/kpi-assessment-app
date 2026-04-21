from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import os
import re
import math
import shutil
import pandas as pd
from typing import Optional, Dict, List, Any
from fastapi import UploadFile, File
from openpyxl import load_workbook
from io import BytesIO
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
from datetime import datetime
FINANCE_KEYWORDS = {
    "revenue": [
        "revenue", "sales", "income", "total sales", "net sales", "turnover"
    ],
    "cogs": [
        "cogs", "cost of goods sold", "cost of sales", "direct cost", "cost"
    ],
    "operating_expenses": [
        "operating expenses", "opex", "expenses", "operational expenses"
    ],
    "other_expenses": [
        "other expenses", "non operating expenses", "misc expenses", "additional expenses"
    ],
    "current_assets": [
        "current assets", "cash", "bank", "inventory", "receivables", "accounts receivable"
    ],
    "fixed_assets": [
        "fixed assets", "property", "equipment", "ppe", "non current assets"
    ],
    "current_liabilities": [
        "current liabilities", "payables", "accounts payable", "short term liabilities"
    ],
    "long_term_liabilities": [
        "long term liabilities", "non current liabilities", "loans", "long term debt"
    ],
    "cash_in": [
        "cash in", "cash inflow", "inflow", "receipts", "collections"
    ],
    "cash_out": [
        "cash out", "cash outflow", "outflow", "payments", "disbursements"
    ],
    "investing_cash_flow": [
        "investing cash flow", "investment cash flow", "cash from investing"
    ],
    "financing_cash_flow": [
        "financing cash flow", "cash from financing"
    ],
}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = text.replace("&", " and ")
    text = text.replace("_", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_number(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return not (isinstance(value, float) and math.isnan(value))
    try:
        cleaned = str(value).replace(",", "").replace("EGP", "").replace("$", "").strip()
        float(cleaned)
        return True
    except Exception:
        return False


def to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return 0.0
        return float(value)

    text = str(value).strip()
    text = text.replace(",", "")
    text = text.replace("EGP", "")
    text = text.replace("$", "")
    text = text.replace("%", "")
    text = text.strip()

    try:
        return float(text)
    except Exception:
        return 0.0


def find_best_sheet(excel_file: pd.ExcelFile) -> str:
    best_sheet = excel_file.sheet_names[0]
    best_score = -1

    for sheet_name in excel_file.sheet_names:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            score = df.shape[0] * 2 + df.shape[1]

            joined_cols = " ".join([normalize_text(c) for c in df.columns])
            keyword_hits = 0
            for words in FINANCE_KEYWORDS.values():
                for word in words:
                    if normalize_text(word) in joined_cols:
                        keyword_hits += 1

            score += keyword_hits * 10

            if score > best_score:
                best_score = score
                best_sheet = sheet_name
        except Exception:
            continue

    return best_sheet


def find_column_by_header(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    normalized_columns = {col: normalize_text(col) for col in df.columns}

    for keyword in keywords:
        keyword_norm = normalize_text(keyword)
        for original_col, normalized_col in normalized_columns.items():
            if keyword_norm == normalized_col:
                return original_col

    for keyword in keywords:
        keyword_norm = normalize_text(keyword)
        for original_col, normalized_col in normalized_columns.items():
            if keyword_norm in normalized_col:
                return original_col

    return None


def find_label_and_value_rows(df: pd.DataFrame, keywords: List[str]) -> Optional[float]:
    if df.empty:
        return None

    for _, row in df.iterrows():
        row_values = list(row.values)
        row_texts = [normalize_text(v) for v in row_values]

        for keyword in keywords:
            keyword_norm = normalize_text(keyword)

            for i, cell_text in enumerate(row_texts):
                if keyword_norm and keyword_norm in cell_text:
                    for j, cell_value in enumerate(row_values):
                        if j != i and is_number(cell_value):
                            return to_float(cell_value)

    return None


def extract_value_from_column(df: pd.DataFrame, column_name: str) -> float:
    numeric_values = df[column_name].apply(to_float)
    return float(numeric_values.sum())


def detect_finance_data(df: pd.DataFrame) -> Dict[str, Any]:
    result = {}
    detected_columns = {}

    for metric, keywords in FINANCE_KEYWORDS.items():
        matched_col = find_column_by_header(df, keywords)
        if matched_col:
            detected_columns[metric] = matched_col
            result[metric] = extract_value_from_column(df, matched_col)
        else:
            result[metric] = None

    for metric, current_value in result.items():
        if current_value is None:
            row_value = find_label_and_value_rows(df, FINANCE_KEYWORDS[metric])
            result[metric] = row_value if row_value is not None else 0.0

    revenue = result.get("revenue", 0.0) or 0.0
    cogs = result.get("cogs", 0.0) or 0.0
    operating_expenses = result.get("operating_expenses", 0.0) or 0.0
    current_assets = result.get("current_assets", 0.0) or 0.0
    current_liabilities = result.get("current_liabilities", 0.0) or 0.0

    result["gross_profit"] = revenue - cogs
    result["net_profit_estimate"] = revenue - cogs - operating_expenses
    result["current_ratio"] = (
        current_assets / current_liabilities if current_liabilities not in [0, 0.0] else 0
    )

    result["detected_columns"] = detected_columns
    return result


def read_finance_excel_flexible(file_path: str) -> Dict[str, Any]:
    excel_file = pd.ExcelFile(file_path)
    best_sheet = find_best_sheet(excel_file)
    df = pd.read_excel(excel_file, sheet_name=best_sheet)

    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    finance_data = detect_finance_data(df)
    finance_data["sheet_used"] = best_sheet
    finance_data["columns_found_in_sheet"] = [str(col) for col in df.columns]

    return finance_data
app = FastAPI(
    title="KPI Assessment API",
    version="1.0.0"
)
app.mount("/static", StaticFiles(directory="static"), name="static")

def init_db():
    conn = sqlite3.connect("kpi.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS assessments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT,
        overall_score REAL,
        overall_status TEXT,
        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()
    
init_db()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5501",
        "http://localhost:5501",
        "http://127.0.0.1:5500",
        "http://localhost:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class KPIInput(BaseModel):
    company_name: str = "Company"

    current_revenue: float
    previous_revenue: float
    leads_count: int
    converted_customers: int
    orders_count: int
    sales_target: float

    revenue: float
    cogs: float
    total_expenses: float
    accounts_receivable: float
    average_daily_sales: float
    cash_in: float
    cash_out: float

    total_orders: int
    on_time_orders: int
    completed_orders: int
    error_cases: int

    average_inventory: float
    accurate_count_items: int
    total_counted_items: int
    out_of_stock_cases: int
    total_sku_requests: int

class LoginInput(BaseModel):
    username: str
    password: str

@app.get("/")
def home():
    return FileResponse("static/index.html")


def score_high_better(value, excellent, good, average):
    if value >= excellent:
        return 100, "green"
    elif value >= good:
        return 80, "yellow"
    elif value >= average:
        return 60, "yellow"
    return 40, "red"


def score_low_better(value, excellent, good, average):
    if value <= excellent:
        return 100, "green"
    elif value <= good:
        return 80, "yellow"
    elif value <= average:
        return 60, "yellow"
    return 40, "red"


def get_status(score):
    if score >= 85:
        return "Excellent", "green"
    elif score >= 70:
        return "Good", "yellow"
    return "Needs Improvement", "red"


@app.post("/kpi/calculate")
def calculate_kpi(data: KPIInput):
    # -------------------------
    # KPI Calculations
    # -------------------------
    sales_growth = ((data.current_revenue - data.previous_revenue) / data.previous_revenue * 100) if data.previous_revenue != 0 else 0
    conversion_rate = (data.converted_customers / data.leads_count * 100) if data.leads_count != 0 else 0
    average_order_value = (data.current_revenue / data.orders_count) if data.orders_count != 0 else 0
    target_achievement = (data.current_revenue / data.sales_target * 100) if data.sales_target != 0 else 0

    gross_profit_margin = ((data.revenue - data.cogs) / data.revenue * 100) if data.revenue != 0 else 0
    collection_period = (data.accounts_receivable / data.average_daily_sales) if data.average_daily_sales != 0 else 0
    expense_ratio = (data.total_expenses / data.revenue * 100) if data.revenue != 0 else 0
    cash_flow = data.cash_in - data.cash_out

    on_time_delivery = (data.on_time_orders / data.total_orders * 100) if data.total_orders != 0 else 0
    fulfillment_rate = (data.completed_orders / data.total_orders * 100) if data.total_orders != 0 else 0
    error_rate = (data.error_cases / data.total_orders * 100) if data.total_orders != 0 else 0

    inventory_turnover = (data.cogs / data.average_inventory) if data.average_inventory != 0 else 0
    stock_accuracy = (data.accurate_count_items / data.total_counted_items * 100) if data.total_counted_items != 0 else 0
    out_of_stock_rate = (data.out_of_stock_cases / data.total_sku_requests * 100) if data.total_sku_requests != 0 else 0

    # -------------------------
    # Scoring
    # -------------------------
    sg_score, _ = score_high_better(sales_growth, 20, 10, 0)
    cr_score, _ = score_high_better(conversion_rate, 30, 20, 10)
    ta_score, _ = score_high_better(target_achievement, 100, 80, 60)

    gpm_score, _ = score_high_better(gross_profit_margin, 30, 20, 10)
    cp_score, _ = score_low_better(collection_period, 30, 45, 60)
    er_score, _ = score_low_better(expense_ratio, 30, 45, 60)
    cf_score = 100 if cash_flow > 0 else 40

    otd_score, _ = score_high_better(on_time_delivery, 95, 85, 75)
    fr_score, _ = score_high_better(fulfillment_rate, 95, 85, 75)
    err_score, _ = score_low_better(error_rate, 2, 5, 10)

    it_score, _ = score_high_better(inventory_turnover, 8, 5, 3)
    sa_score, _ = score_high_better(stock_accuracy, 98, 95, 90)
    os_score, _ = score_low_better(out_of_stock_rate, 2, 5, 10)

    sales_score = round((sg_score + cr_score + ta_score) / 3, 2)
    finance_score = round((gpm_score + cp_score + er_score + cf_score) / 4, 2)
    operations_score = round((otd_score + fr_score + err_score) / 3, 2)
    inventory_score = round((it_score + sa_score + os_score) / 3, 2)

    overall_score = round((sales_score + finance_score + operations_score + inventory_score) / 4, 2)

    sales_status, sales_color = get_status(sales_score)
    finance_status, finance_color = get_status(finance_score)
    operations_status, operations_color = get_status(operations_score)
    inventory_status, inventory_color = get_status(inventory_score)
    overall_status, overall_color = get_status(overall_score)

    # -------------------------
    # Recommendations
    # -------------------------
    recommendations = []

    if sales_score < 70:
        recommendations.append("Improve lead conversion and track sales target achievement weekly.")
    if finance_score < 70:
        recommendations.append("Review collection cycle, expense ratio, and strengthen cash flow control.")
    if operations_score < 70:
        recommendations.append("Improve on-time delivery and reduce process errors.")
    if inventory_score < 70:
        recommendations.append("Improve stock accuracy and reduce out-of-stock incidents.")

    if not recommendations:
        recommendations.append("Overall performance is stable. Focus on continuous monitoring and incremental improvement.")

    return {
        "_name": data.company_name,
        "sales": {
            "score": sales_score,
            "status": sales_status,
            "color": sales_color,
            "kpis": {
                "sales_growth": round(sales_growth, 2),
                "conversion_rate": round(conversion_rate, 2),
                "average_order_value": round(average_order_value, 2),
                "target_achievement": round(target_achievement, 2),
            },
        },
        "finance": {
            "score": finance_score,
            "status": finance_status,
            "color": finance_color,
            "kpis": {
                "gross_profit_margin": round(gross_profit_margin, 2),
                "collection_period": round(collection_period, 2),
                "expense_ratio": round(expense_ratio, 2),
                "cash_flow": round(cash_flow, 2),
            },
        },
        "operations": {
            "score": operations_score,
            "status": operations_status,
            "color": operations_color,
            "kpis": {
                "on_time_delivery": round(on_time_delivery, 2),
                "fulfillment_rate": round(fulfillment_rate, 2),
                "error_rate": round(error_rate, 2),
            },
        },
        "inventory": {
            "score": inventory_score,
            "status": inventory_status,
            "color": inventory_color,
            "kpis": {
                "inventory_turnover": round(inventory_turnover, 2),
                "stock_accuracy": round(stock_accuracy, 2),
                "out_of_stock_rate": round(out_of_stock_rate, 2),
            },
        },
        "overall": {
            "score": overall_score,
            "status": overall_status,
            "color": overall_color,
        },
        "recommendations": recommendations,
    }
# Fake Database 
assessments_db = []


@app.post("/kpi/save")
def save_assessment(data: KPIInput):
    result = calculate_kpi(data)

    record = {
        "company_name": data.company_name,
        "result": result
    }

    assessments_db.append(record)

    return {"message": "Assessment saved successfully"}


@app.get("/kpi/list")
def list_assessments():
    return {"companies": [item["company_name"] for item in assessments_db]}


@app.get("/kpi/company/{name}")
def get_company(name: str):
    for item in assessments_db:
        if item["company_name"] == name:
            return item["result"]

    return {"error": "Company not found"}
@app.post("/login")
def login(data: LoginInput):
    users = [
        {"username": "admin", "password": "1234", "role": "Admin", "fullName": "Ahmed Eissa"},
        {"username": "manager", "password": "1234", "role": "Manager", "fullName": "Operations Manager"},
        {"username": "employee", "password": "1234", "role": "Employee", "fullName": "Staff User"},
    ]

    for user in users:
        if user["username"] == data.username and user["password"] == data.password:
            return {
                "success": True,
                "user": {
                    "username": user["username"],
                    "role": user["role"],
                    "fullName": user["fullName"]
                }
            }

    return {"success": False, "message": "Wrong username or password"}
@app.post("/upload-finance")
async def upload_finance(file: UploadFile = File(...)):
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)

    file_path = os.path.join(upload_dir, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        finance_data = read_finance_excel_flexible(file_path)

        return {
            "success": True,
            "message": "Finance file processed successfully",
            "data": finance_data
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Error processing finance file: {str(e)}"
        }