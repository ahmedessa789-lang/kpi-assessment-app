import os
import re
import math
import shutil
import sqlite3
from datetime import datetime
from typing import Optional, Dict, List, Any

import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


FINANCE_KEYWORDS = {
    "revenue": ["revenue", "sales", "income", "total sales", "net sales", "turnover"],
    "cogs": ["cogs", "cost of goods sold", "cost of sales", "direct cost", "cost"],
    "operating_expenses": ["operating expenses", "opex", "expenses", "operational expenses"],
    "other_expenses": ["other expenses", "non operating expenses", "misc expenses", "additional expenses"],
    "current_assets": ["current assets", "cash", "bank", "inventory", "receivables", "accounts receivable"],
    "fixed_assets": ["fixed assets", "property", "equipment", "ppe", "non current assets"],
    "current_liabilities": ["current liabilities", "payables", "accounts payable", "short term liabilities"],
    "long_term_liabilities": ["long term liabilities", "non current liabilities", "loans", "long term debt"],
    "cash_in": ["cash in", "cash inflow", "inflow", "receipts", "collections"],
    "cash_out": ["cash out", "cash outflow", "outflow", "payments", "disbursements"],
    "investing_cash_flow": ["investing cash flow", "investment cash flow", "cash from investing"],
    "financing_cash_flow": ["financing cash flow", "cash from financing"],
}

CEO_KEYWORDS = {
    "revenue": ["revenue", "sales", "income", "current revenue", "total revenue"],
    "leads": ["leads", "lead count", "prospects", "inquiries"],
    "orders": ["orders", "orders count", "total orders", "sales orders"],
    "expenses": ["expenses", "total expenses", "costs", "operating expenses"],
    "inventory": ["inventory", "avg inventory", "average inventory", "stock"],
    "cash": ["cash", "cash in", "cash balance", "available cash"],
}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = text.replace("&", " and ").replace("_", " ").replace("-", " ")
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def is_number(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return not (isinstance(value, float) and math.isnan(value))
    try:
        cleaned = str(value).replace(",", "").replace("EGP", "").replace("$", "").replace("%", "").strip()
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
    for token in [",", "EGP", "$", "%"]:
        text = text.replace(token, "")
    try:
        return float(text.strip())
    except Exception:
        return 0.0


def safe_div(numerator: float, denominator: float, percent: bool = False) -> float:
    if denominator in [0, 0.0]:
        return 0.0
    result = numerator / denominator
    return result * 100 if percent else result


def find_best_sheet_generic(excel_file: pd.ExcelFile, keyword_dict: Dict[str, List[str]]) -> str:
    best_sheet = excel_file.sheet_names[0]
    best_score = -1

    for sheet_name in excel_file.sheet_names:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            score = df.shape[0] * 2 + df.shape[1]
            joined_cols = " ".join(normalize_text(c) for c in df.columns)

            keyword_hits = 0
            for words in keyword_dict.values():
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
    return float(df[column_name].apply(to_float).sum())


def detect_finance_data(df: pd.DataFrame) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    detected_columns: Dict[str, str] = {}

    for metric, keywords in FINANCE_KEYWORDS.items():
        matched_col = find_column_by_header(df, keywords)
        if matched_col:
            detected_columns[metric] = matched_col
            result[metric] = extract_value_from_column(df, matched_col)
        else:
            result[metric] = None

    for metric, current_value in list(result.items()):
        if current_value is None:
            row_value = find_label_and_value_rows(df, FINANCE_KEYWORDS[metric])
            result[metric] = row_value if row_value is not None else 0.0

    revenue = result.get("revenue", 0.0) or 0.0
    cogs = result.get("cogs", 0.0) or 0.0
    operating_expenses = result.get("operating_expenses", 0.0) or 0.0
    other_expenses = result.get("other_expenses", 0.0) or 0.0
    current_assets = result.get("current_assets", 0.0) or 0.0
    fixed_assets = result.get("fixed_assets", 0.0) or 0.0
    current_liabilities = result.get("current_liabilities", 0.0) or 0.0
    long_term_liabilities = result.get("long_term_liabilities", 0.0) or 0.0
    cash_in = result.get("cash_in", 0.0) or 0.0
    cash_out = result.get("cash_out", 0.0) or 0.0
    investing_cash_flow = result.get("investing_cash_flow", 0.0) or 0.0
    financing_cash_flow = result.get("financing_cash_flow", 0.0) or 0.0

    total_assets = current_assets + fixed_assets
    total_liabilities = current_liabilities + long_term_liabilities
    equity = total_assets - total_liabilities
    gross_profit = revenue - cogs
    operating_profit = gross_profit - operating_expenses
    net_profit = operating_profit - other_expenses
    net_cash_flow = cash_in - cash_out + investing_cash_flow + financing_cash_flow

    result.update(
        {
            "gross_profit": round(gross_profit, 2),
            "operating_profit": round(operating_profit, 2),
            "net_profit": round(net_profit, 2),
            "total_assets": round(total_assets, 2),
            "total_liabilities": round(total_liabilities, 2),
            "equity": round(equity, 2),
            "net_cash_flow": round(net_cash_flow, 2),
            "current_ratio": round(safe_div(current_assets, current_liabilities), 2),
            "quick_ratio": round(safe_div(current_assets, current_liabilities), 2),
            "cash_ratio": round(safe_div(cash_in, current_liabilities), 2),
            "debt_to_equity": round(safe_div(total_liabilities, equity), 2),
            "debt_ratio": round(safe_div(total_liabilities, total_assets), 2),
            "debt_to_assets": round(safe_div(total_liabilities, total_assets), 2),
            "roa": round(safe_div(net_profit, total_assets, percent=True), 2),
            "roe": round(safe_div(net_profit, equity, percent=True), 2),
            "gross_margin": round(safe_div(gross_profit, revenue, percent=True), 2),
            "operating_margin": round(safe_div(operating_profit, revenue, percent=True), 2),
            "net_margin": round(safe_div(net_profit, revenue, percent=True), 2),
            "working_capital": round(current_assets - current_liabilities, 2),
            "asset_turnover": round(safe_div(revenue, total_assets), 2),
            "inventory_turnover": round(safe_div(cogs, current_assets), 2),
            "receivables_turnover": round(safe_div(revenue, cash_in), 2),
            "detected_columns": detected_columns,
        }
    )
    return result


def detect_ceo_data(df: pd.DataFrame) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    detected_columns: Dict[str, str] = {}

    for metric, keywords in CEO_KEYWORDS.items():
        matched_col = find_column_by_header(df, keywords)
        if matched_col:
            detected_columns[metric] = matched_col
            result[metric] = extract_value_from_column(df, matched_col)
        else:
            result[metric] = None

    for metric, current_value in list(result.items()):
        if current_value is None:
            row_value = find_label_and_value_rows(df, CEO_KEYWORDS[metric])
            result[metric] = row_value if row_value is not None else 0.0

    result["detected_columns"] = detected_columns
    return result


def read_finance_excel_flexible(file_path: str) -> Dict[str, Any]:
    excel_file = pd.ExcelFile(file_path)
    best_sheet = find_best_sheet_generic(excel_file, FINANCE_KEYWORDS)
    df = pd.read_excel(excel_file, sheet_name=best_sheet)
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    finance_data = detect_finance_data(df)
    finance_data["sheet_used"] = best_sheet
    finance_data["columns_found_in_sheet"] = [str(col) for col in df.columns]
    return finance_data


def read_ceo_excel_flexible(file_path: str) -> Dict[str, Any]:
    excel_file = pd.ExcelFile(file_path)
    best_sheet = find_best_sheet_generic(excel_file, CEO_KEYWORDS)
    df = pd.read_excel(excel_file, sheet_name=best_sheet)
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    ceo_data = detect_ceo_data(df)
    ceo_data["sheet_used"] = best_sheet
    ceo_data["columns_found_in_sheet"] = [str(col) for col in df.columns]
    return ceo_data


app = FastAPI(title="KPI Assessment API", version="2.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")


def init_db() -> None:
    conn = sqlite3.connect("kpi.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS assessments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT,
            overall_score REAL,
            overall_status TEXT,
            created_at TEXT
        )
        """
    )
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


class FinanceInput(BaseModel):
    revenue: float = 0
    cogs: float = 0
    operating_expenses: float = 0
    other_expenses: float = 0
    current_assets: float = 0
    fixed_assets: float = 0
    current_liabilities: float = 0
    long_term_liabilities: float = 0
    cash_in: float = 0
    cash_out: float = 0
    investing_cash_flow: float = 0
    financing_cash_flow: float = 0
    accounts_receivable: float = 0


class LoginInput(BaseModel):
    username: str
    password: str


def score_high_better(value: float, excellent: float, good: float, average: float):
    if value >= excellent:
        return 100, "green"
    if value >= good:
        return 80, "yellow"
    if value >= average:
        return 60, "yellow"
    return 40, "red"


def score_low_better(value: float, excellent: float, good: float, average: float):
    if value <= excellent:
        return 100, "green"
    if value <= good:
        return 80, "yellow"
    if value <= average:
        return 60, "yellow"
    return 40, "red"


def get_status(score: float):
    if score >= 85:
        return "Excellent", "green"
    if score >= 70:
        return "Good", "yellow"
    return "Needs Improvement", "red"


@app.get("/")
def home():
    return FileResponse("static/index.html")


@app.post("/upload-ceo")
async def upload_ceo(file: UploadFile = File(...)):
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        ceo_data = read_ceo_excel_flexible(file_path)
        return {"success": True, "message": "CEO file processed successfully", "data": ceo_data}
    except Exception as e:
        return {"success": False, "message": f"Error processing CEO file: {str(e)}"}


@app.post("/upload-finance")
async def upload_finance(file: UploadFile = File(...)):
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        finance_data = read_finance_excel_flexible(file_path)
        return {"success": True, "message": "Finance file processed successfully", "data": finance_data}
    except Exception as e:
        return {"success": False, "message": f"Error processing finance file: {str(e)}"}


@app.post("/finance/analyze")
def analyze_finance(data: FinanceInput):
    revenue = data.revenue
    cogs = data.cogs
    operating_expenses = data.operating_expenses
    other_expenses = data.other_expenses
    current_assets = data.current_assets
    fixed_assets = data.fixed_assets
    current_liabilities = data.current_liabilities
    long_term_liabilities = data.long_term_liabilities
    cash_in = data.cash_in
    cash_out = data.cash_out
    investing_cash_flow = data.investing_cash_flow
    financing_cash_flow = data.financing_cash_flow
    accounts_receivable = data.accounts_receivable

    gross_profit = revenue - cogs
    operating_profit = gross_profit - operating_expenses
    net_profit = operating_profit - other_expenses
    total_assets = current_assets + fixed_assets
    total_liabilities = current_liabilities + long_term_liabilities
    equity = total_assets - total_liabilities
    net_cash_flow = cash_in - cash_out + investing_cash_flow + financing_cash_flow

    result = {
        "gross_profit": round(gross_profit, 2),
        "operating_profit": round(operating_profit, 2),
        "net_profit": round(net_profit, 2),
        "total_assets": round(total_assets, 2),
        "total_liabilities": round(total_liabilities, 2),
        "equity": round(equity, 2),
        "net_cash_flow": round(net_cash_flow, 2),
        "current_ratio": round(safe_div(current_assets, current_liabilities), 2),
        "quick_ratio": round(safe_div(current_assets, current_liabilities), 2),
        "cash_ratio": round(safe_div(cash_in, current_liabilities), 2),
        "debt_to_equity": round(safe_div(total_liabilities, equity), 2),
        "debt_ratio": round(safe_div(total_liabilities, total_assets), 2),
        "debt_to_assets": round(safe_div(total_liabilities, total_assets), 2),
        "roa": round(safe_div(net_profit, total_assets, percent=True), 2),
        "roe": round(safe_div(net_profit, equity, percent=True), 2),
        "gross_margin": round(safe_div(gross_profit, revenue, percent=True), 2),
        "operating_margin": round(safe_div(operating_profit, revenue, percent=True), 2),
        "net_margin": round(safe_div(net_profit, revenue, percent=True), 2),
        "working_capital": round(current_assets - current_liabilities, 2),
        "asset_turnover": round(safe_div(revenue, total_assets), 2),
        "inventory_turnover": round(safe_div(cogs, current_assets), 2),
        "receivables_turnover": round(safe_div(revenue, accounts_receivable), 2),
    }

    comments = []
    if result["current_ratio"] < 1:
        comments.append("Liquidity is weak. Current liabilities may exceed short-term resources.")
    if result["net_margin"] < 0:
        comments.append("The company is generating a negative net margin and needs cost control.")
    if result["debt_to_equity"] > 2:
        comments.append("Leverage is high. Review financing structure and debt exposure.")
    if result["roa"] < 5:
        comments.append("Asset utilization is relatively weak and may need operational improvement.")
    if not comments:
        comments.append("Financial performance looks stable based on the current data.")

    result["ai_summary"] = comments
    return result


@app.post("/kpi/calculate")
def calculate_kpi(data: KPIInput):
    sales_growth = safe_div(data.current_revenue - data.previous_revenue, data.previous_revenue, percent=True)
    conversion_rate = safe_div(data.converted_customers, data.leads_count, percent=True)
    average_order_value = safe_div(data.current_revenue, data.orders_count)
    target_achievement = safe_div(data.current_revenue, data.sales_target, percent=True)

    gross_profit_margin = safe_div(data.revenue - data.cogs, data.revenue, percent=True)
    collection_period = safe_div(data.accounts_receivable, data.average_daily_sales)
    expense_ratio = safe_div(data.total_expenses, data.revenue, percent=True)
    cash_flow = data.cash_in - data.cash_out

    on_time_delivery = safe_div(data.on_time_orders, data.total_orders, percent=True)
    fulfillment_rate = safe_div(data.completed_orders, data.total_orders, percent=True)
    error_rate = safe_div(data.error_cases, data.total_orders, percent=True)

    inventory_turnover = safe_div(data.cogs, data.average_inventory)
    stock_accuracy = safe_div(data.accurate_count_items, data.total_counted_items, percent=True)
    out_of_stock_rate = safe_div(data.out_of_stock_cases, data.total_sku_requests, percent=True)

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
        "company_name": data.company_name,
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
        "overall": {"score": overall_score, "status": overall_status, "color": overall_color},
        "recommendations": recommendations,
    }


assessments_db: List[Dict[str, Any]] = []


@app.post("/kpi/save")
def save_assessment(data: KPIInput):
    result = calculate_kpi(data)
    record = {"company_name": data.company_name, "result": result, "created_at": datetime.now().isoformat()}
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
                    "fullName": user["fullName"],
                },
            }

    return {"success": False, "message": "Wrong username or password"}
