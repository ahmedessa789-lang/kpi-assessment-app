from smart_excel_engine import analyze_excel_file
from odoo_connector import get_sales_orders, calculate_sales_kpi
import os
import re
import math
import shutil
import sqlite3
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Header, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
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


DB_PATH = "kpi.db"


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_password(password: str, stored_hash: str) -> bool:
    return hash_password(password) == stored_hash


def seed_default_users() -> None:
    default_users: List[Tuple[str, str, str, str]] = [
        ("admin", hash_password("1234"), "Admin", "Ahmed Eissa"),
        ("manager", hash_password("1234"), "Manager", "Mohamed Karam"),
        ("finance", hash_password("1234"), "Finance", "Mohamed Elamir"),
    ]

    conn = get_db_connection()
    cursor = conn.cursor()
    for username, password_hash, role, full_name in default_users:
        cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute(
                """
                INSERT INTO users (username, password_hash, role, full_name, is_active, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (username, password_hash, role, full_name, datetime.now().isoformat()),
            )
    conn.commit()
    conn.close()


app = FastAPI(
    title="KPI Assessment API",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


def init_db() -> None:
    conn = get_db_connection()
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            full_name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT UNIQUE NOT NULL,
            username TEXT NOT NULL,
            role TEXT NOT NULL,
            full_name TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            job TEXT,
            request_type TEXT NOT NULL DEFAULT 'Demo Request',
            source TEXT DEFAULT 'Landing Page',
            status TEXT NOT NULL DEFAULT 'New',
            notes TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sales_username TEXT NOT NULL,
            sales_full_name TEXT NOT NULL,
            sale_date TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            product_name TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit_price REAL NOT NULL,
            total_amount REAL NOT NULL,
            payment_method TEXT,
            notes TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS business_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            product_name TEXT NOT NULL,
            company_name TEXT NOT NULL,
            plan TEXT NOT NULL,
            license_status TEXT NOT NULL,
            trial_expires_at TEXT NOT NULL,
            max_users INTEGER NOT NULL DEFAULT 50,
            license_key_hash TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )

    cursor.execute("SELECT id FROM business_settings WHERE id = 1")
    existing_business = cursor.fetchone()
    if not existing_business:
        cursor.execute(
            """
            INSERT INTO business_settings
            (id, product_name, company_name, plan, license_status, trial_expires_at, max_users, license_key_hash, updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Reap Service KPI Dashboard",
                "Demo Company",
                "Trial",
                "trial",
                (datetime.now() + timedelta(days=int(os.getenv("DEFAULT_TRIAL_DAYS", "14")))).isoformat(),
                int(os.getenv("DEFAULT_MAX_USERS", "50")),
                None,
                datetime.now().isoformat(),
            ),
        )

    # Auto-upgrade old trial/user limit from 5 to the configured default.
    # This keeps existing databases from blocking new users after code update.
    default_max_users = int(os.getenv("DEFAULT_MAX_USERS", "50"))
    cursor.execute("SELECT max_users FROM business_settings WHERE id = 1")
    current_business_limit = cursor.fetchone()
    if current_business_limit and int(current_business_limit["max_users"]) < default_max_users:
        cursor.execute(
            "UPDATE business_settings SET max_users = ?, updated_at = ? WHERE id = 1",
            (default_max_users, datetime.now().isoformat()),
        )

    conn.commit()
    conn.close()
    seed_default_users()


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


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cache-Control"] = "no-store"
    return response


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


class UserCreateInput(BaseModel):
    username: str
    password: str
    role: str
    full_name: str


class ChangePasswordInput(BaseModel):
    current_password: str
    new_password: str


class AdminChangePasswordInput(BaseModel):
    username: str
    new_password: str


class BusinessLicenseInput(BaseModel):
    company_name: str = "Demo Company"
    product_name: str = "Reap Service KPI Dashboard"
    plan: str = "Trial"
    license_status: str = "trial"
    trial_days: int = 14
    max_users: int = 50
    license_key: Optional[str] = None



class LeadInput(BaseModel):
    name: str
    phone: str
    job: str = ""
    request_type: str = "Demo Request"
    source: str = "Landing Page"


class SalesCreateInput(BaseModel):
    sale_date: str = ""
    sales_username: Optional[str] = None
    customer_name: str
    product_name: str
    quantity: float
    unit_price: float
    payment_method: str = "Cash"
    notes: str = ""


class SalesReportFilter(BaseModel):
    sales_username: Optional[str] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None

SESSION_HOURS = int(os.getenv("SESSION_HOURS", "8"))
MAX_LOGIN_ATTEMPTS = int(os.getenv("MAX_LOGIN_ATTEMPTS", "5"))
LOGIN_BLOCK_MINUTES = int(os.getenv("LOGIN_BLOCK_MINUTES", "10"))
ALLOWED_UPLOAD_EXTENSIONS = {".xlsx", ".xls", ".csv"}
MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "8"))
login_attempts: Dict[str, Dict[str, Any]] = {}


def get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def clean_old_attempts() -> None:
    now = datetime.now()
    expired_keys = []
    for key, item in login_attempts.items():
        blocked_until = item.get("blocked_until")
        last_attempt = item.get("last_attempt")
        if blocked_until and blocked_until > now:
            continue
        if last_attempt and (now - last_attempt).total_seconds() > LOGIN_BLOCK_MINUTES * 60:
            expired_keys.append(key)
    for key in expired_keys:
        login_attempts.pop(key, None)


def check_login_rate_limit(request: Request, username: str) -> None:
    clean_old_attempts()
    key = f"{get_client_ip(request)}:{username.lower()}"
    item = login_attempts.get(key)
    if item and item.get("blocked_until") and item["blocked_until"] > datetime.now():
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")


def record_login_failure(request: Request, username: str) -> None:
    key = f"{get_client_ip(request)}:{username.lower()}"
    item = login_attempts.setdefault(key, {"count": 0, "last_attempt": datetime.now(), "blocked_until": None})
    item["count"] += 1
    item["last_attempt"] = datetime.now()
    if item["count"] >= MAX_LOGIN_ATTEMPTS:
        item["blocked_until"] = datetime.now() + timedelta(minutes=LOGIN_BLOCK_MINUTES)


def clear_login_failures(request: Request, username: str) -> None:
    key = f"{get_client_ip(request)}:{username.lower()}"
    login_attempts.pop(key, None)


def validate_upload(file: UploadFile) -> None:
    filename = os.path.basename(file.filename or "")
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only Excel/CSV files are allowed")
    content_length = file.headers.get("content-length") if file.headers else None
    if content_length and int(content_length) > MAX_UPLOAD_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail=f"File too large. Max {MAX_UPLOAD_SIZE_MB} MB")


def normalize_role(role: str) -> str:
    role_clean = (role or "").strip().lower()
    if role_clean == "admin":
        return "Admin"
    if role_clean == "finance":
        return "Finance"
    if role_clean == "manager":
        return "Manager"
    if role_clean == "employee":
        return "Employee"
    if role_clean in {"sales", "sales rep", "sales representative"}:
        return "Sales"
    if role_clean in {"sales manager", "salesmanager"}:
        return "Sales Manager"
    return role.strip().title()


def create_session_token(username: str, role: str, full_name: str) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now() + timedelta(hours=SESSION_HOURS)).isoformat()

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sessions (token, username, role, full_name, expires_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (token, username, normalize_role(role), full_name, expires_at, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()
    return token


def get_current_user(
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    token = x_auth_token

    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]

    if not token:
        raise HTTPException(status_code=401, detail="Login required")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT token, username, role, full_name, expires_at
        FROM sessions
        WHERE token = ?
        """,
        (token,),
    )
    session = cursor.fetchone()
    conn.close()

    if not session:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    expires_at = datetime.fromisoformat(session["expires_at"])
    if expires_at < datetime.now():
        raise HTTPException(status_code=401, detail="Session expired. Please login again.")

    return {
        "username": session["username"],
        "role": normalize_role(session["role"]),
        "fullName": session["full_name"],
    }


def require_roles(*allowed_roles: str):
    allowed = {normalize_role(role) for role in allowed_roles}

    def checker(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        if normalize_role(user.get("role", "")) not in allowed:
            raise HTTPException(status_code=403, detail="You do not have permission to access this action")
        return user

    return checker


def get_business_settings() -> Dict[str, Any]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT product_name, company_name, plan, license_status, trial_expires_at, max_users, updated_at
        FROM business_settings
        WHERE id = 1
        """
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return {
            "productName": "Reap Service KPI Dashboard",
            "companyName": "Demo Company",
            "plan": "Trial",
            "licenseStatus": "trial",
            "trialExpiresAt": (datetime.now() + timedelta(days=14)).isoformat(),
            "maxUsers": 5,
            "updatedAt": datetime.now().isoformat(),
            "daysLeft": 14,
            "isActive": True,
        }

    trial_expires = datetime.fromisoformat(row["trial_expires_at"])
    days_left = max(0, (trial_expires - datetime.now()).days)
    status = (row["license_status"] or "trial").lower()
    is_active = status in {"active", "paid"} or (status == "trial" and trial_expires >= datetime.now())

    return {
        "productName": row["product_name"],
        "companyName": row["company_name"],
        "plan": row["plan"],
        "licenseStatus": status,
        "trialExpiresAt": row["trial_expires_at"],
        "maxUsers": int(row["max_users"]),
        "updatedAt": row["updated_at"],
        "daysLeft": days_left,
        "isActive": is_active,
    }


def count_active_users() -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS total FROM users WHERE is_active = 1")
    row = cursor.fetchone()
    conn.close()
    return int(row["total"] if row else 0)


def make_license_hash(license_key: Optional[str]) -> Optional[str]:
    if not license_key:
        return None
    secret = os.getenv("LICENSE_SECRET", "change-this-license-secret")
    return hashlib.sha256((license_key.strip() + secret).encode("utf-8")).hexdigest()


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

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse("static/favicon.ico")

@app.get("/landing")
def landing():
    return FileResponse("static/landing.html")


@app.post("/upload-ceo")
async def upload_ceo(file: UploadFile = File(...), user: Dict[str, Any] = Depends(require_roles("Admin", "Manager"))):
    validate_upload(file)
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = os.path.basename(file.filename or "upload.xlsx")
    file_path = os.path.join(upload_dir, safe_name)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        ceo_data = read_ceo_excel_flexible(file_path)
        return {"success": True, "message": "CEO file processed successfully", "data": ceo_data}
    except Exception as e:
        return {"success": False, "message": f"Error processing CEO file: {str(e)}"}


@app.post("/upload-finance")
async def upload_finance(file: UploadFile = File(...), user: Dict[str, Any] = Depends(require_roles("Admin", "Finance"))):
    validate_upload(file)
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)
    safe_name = os.path.basename(file.filename or "upload.xlsx")
    file_path = os.path.join(upload_dir, safe_name)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        finance_data = read_finance_excel_flexible(file_path)
        return {"success": True, "message": "Finance file processed successfully", "data": finance_data}
    except Exception as e:
        return {"success": False, "message": f"Error processing finance file: {str(e)}"}


@app.post("/finance/analyze")
def analyze_finance(data: FinanceInput, user: Dict[str, Any] = Depends(require_roles("Admin", "Finance"))):
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
def calculate_kpi(data: KPIInput, user: Dict[str, Any] = Depends(require_roles("Admin", "Manager"))):
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
def save_assessment(data: KPIInput, user: Dict[str, Any] = Depends(require_roles("Admin", "Manager"))):
    result = calculate_kpi(data, user)
    record = {"company_name": data.company_name, "result": result, "created_at": datetime.now().isoformat()}
    assessments_db.append(record)
    return {"message": "Assessment saved successfully"}


@app.get("/kpi/list")
def list_assessments(user: Dict[str, Any] = Depends(require_roles("Admin", "Manager"))):
    return {"companies": [item["company_name"] for item in assessments_db]}


@app.get("/kpi/company/{name}")
def get_company(name: str, user: Dict[str, Any] = Depends(require_roles("Admin", "Manager"))):
    for item in assessments_db:
        if item["company_name"] == name:
            return item["result"]
    return {"error": "Company not found"}


@app.post("/users")
def create_user(data: UserCreateInput, user: Dict[str, Any] = Depends(require_roles("Admin"))):
    allowed_roles = {"Admin", "Manager", "Finance", "Employee", "Sales", "Sales Manager"}
    role = normalize_role(data.role)

    if role not in allowed_roles:
        return {"success": False, "message": "Role must be Admin, Manager, Finance, Employee, Sales, or Sales Manager"}

    username = data.username.strip().lower()
    full_name = data.full_name.strip()

    if not username or not data.password.strip() or not full_name:
        return {"success": False, "message": "Username, password, and full name are required"}

    business = get_business_settings()
    if count_active_users() >= business.get("maxUsers", 5):
        return {"success": False, "message": f"User limit reached for current plan. Max users: {business.get('maxUsers', 5)}"}

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO users (username, password_hash, role, full_name, is_active, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            (username, hash_password(data.password.strip()), role, full_name, datetime.now().isoformat()),
        )
        conn.commit()
        return {
            "success": True,
            "message": "User created successfully",
            "user": {"username": username, "role": role, "fullName": full_name},
        }
    except sqlite3.IntegrityError:
        return {"success": False, "message": "Username already exists"}
    finally:
        conn.close()


@app.get("/users/list")
def list_users(user: Dict[str, Any] = Depends(require_roles("Admin"))):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT username, role, full_name, is_active, created_at
        FROM users
        ORDER BY id ASC
        """
    )
    rows = cursor.fetchall()
    conn.close()

    users = [
        {
            "username": row["username"],
            "role": row["role"],
            "fullName": row["full_name"],
            "isActive": bool(row["is_active"]),
            "createdAt": row["created_at"],
        }
        for row in rows
    ]
    return {"users": users}


@app.get("/me")
def me(user: Dict[str, Any] = Depends(get_current_user)):
    return {"success": True, "user": user}


@app.post("/logout")
def logout(user: Dict[str, Any] = Depends(get_current_user), authorization: Optional[str] = Header(default=None), x_auth_token: Optional[str] = Header(default=None)):
    token = x_auth_token
    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]

    if token:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()
        conn.close()

    return {"success": True, "message": "Logged out successfully"}


@app.post("/change-password")
def change_own_password(data: ChangePasswordInput, user: Dict[str, Any] = Depends(get_current_user)):
    if len(data.new_password.strip()) < 8:
        return {"success": False, "message": "New password must be at least 8 characters"}

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT password_hash FROM users WHERE username = ?", (user["username"],))
    row = cursor.fetchone()
    if not row or not verify_password(data.current_password.strip(), row["password_hash"]):
        conn.close()
        return {"success": False, "message": "Current password is incorrect"}

    cursor.execute(
        "UPDATE users SET password_hash = ? WHERE username = ?",
        (hash_password(data.new_password.strip()), user["username"]),
    )
    cursor.execute("DELETE FROM sessions WHERE username = ?", (user["username"],))
    conn.commit()
    conn.close()
    return {"success": True, "message": "Password changed. Please login again."}


@app.post("/admin/change-user-password")
def admin_change_user_password(data: AdminChangePasswordInput, user: Dict[str, Any] = Depends(require_roles("Admin"))):
    username = data.username.strip().lower()
    if len(data.new_password.strip()) < 8:
        return {"success": False, "message": "New password must be at least 8 characters"}

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE users SET password_hash = ? WHERE username = ?",
        (hash_password(data.new_password.strip()), username),
    )
    updated = cursor.rowcount
    cursor.execute("DELETE FROM sessions WHERE username = ?", (username,))
    conn.commit()
    conn.close()

    if updated == 0:
        return {"success": False, "message": "User not found"}
    return {"success": True, "message": "User password changed and old sessions logged out"}



@app.post("/admin/toggle-user-status")
def admin_toggle_user_status(data: AdminChangePasswordInput, user: Dict[str, Any] = Depends(require_roles("Admin"))):
    username = data.username.strip().lower()
    requested_status = data.new_password.strip().lower()

    if username == "admin":
        return {"success": False, "message": "Main admin account cannot be disabled"}

    if username == user.get("username", "").strip().lower():
        return {"success": False, "message": "You cannot disable your own active session account"}

    if requested_status not in {"active", "inactive", "1", "0", "true", "false"}:
        return {"success": False, "message": "Invalid status value"}

    is_active = 1 if requested_status in {"active", "1", "true"} else 0

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET is_active = ? WHERE username = ?", (is_active, username))
    updated = cursor.rowcount
    if is_active == 0:
        cursor.execute("DELETE FROM sessions WHERE username = ?", (username,))
    conn.commit()
    conn.close()

    if updated == 0:
        return {"success": False, "message": "User not found"}

    return {"success": True, "message": "User status updated successfully"}


@app.delete("/admin/users/{username}")
def admin_delete_user(username: str, user: Dict[str, Any] = Depends(require_roles("Admin"))):
    username_clean = username.strip().lower()

    if username_clean == "admin":
        return {"success": False, "message": "Main admin account cannot be deleted"}

    if username_clean == user.get("username", "").strip().lower():
        return {"success": False, "message": "You cannot delete your own account while logged in"}

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE username = ?", (username_clean,))
    deleted = cursor.rowcount
    cursor.execute("DELETE FROM sessions WHERE username = ?", (username_clean,))
    conn.commit()
    conn.close()

    if deleted == 0:
        return {"success": False, "message": "User not found"}

    return {"success": True, "message": "User deleted successfully"}


def build_sales_summary(rows: List[sqlite3.Row]) -> Dict[str, Any]:
    total_amount = sum(float(row["total_amount"] or 0) for row in rows)
    total_quantity = sum(float(row["quantity"] or 0) for row in rows)
    transactions = len(rows)
    avg_sale = safe_div(total_amount, transactions)
    product_totals: Dict[str, float] = {}
    rep_totals: Dict[str, float] = {}

    for row in rows:
        product = row["product_name"] or "-"
        rep = row["sales_full_name"] or row["sales_username"] or "-"
        product_totals[product] = product_totals.get(product, 0.0) + float(row["quantity"] or 0)
        rep_totals[rep] = rep_totals.get(rep, 0.0) + float(row["total_amount"] or 0)

    top_product = max(product_totals.items(), key=lambda item: item[1])[0] if product_totals else "-"
    top_sales_rep = max(rep_totals.items(), key=lambda item: item[1])[0] if rep_totals else "-"

    return {
        "totalAmount": round(total_amount, 2),
        "totalQuantity": round(total_quantity, 2),
        "transactions": transactions,
        "averageSale": round(avg_sale, 2),
        "topProduct": top_product,
        "topSalesRep": top_sales_rep,
    }


def sales_rows_to_response(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [
        {
            "id": row["id"],
            "salesUsername": row["sales_username"],
            "salesFullName": row["sales_full_name"],
            "saleDate": row["sale_date"],
            "customerName": row["customer_name"],
            "productName": row["product_name"],
            "quantity": float(row["quantity"] or 0),
            "unitPrice": float(row["unit_price"] or 0),
            "totalAmount": float(row["total_amount"] or 0),
            "paymentMethod": row["payment_method"] or "",
            "notes": row["notes"] or "",
            "createdAt": row["created_at"],
        }
        for row in rows
    ]


def get_sales_user_display(username: str) -> Tuple[str, str]:
    username_clean = (username or "").strip().lower()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT username, full_name, role, is_active FROM users WHERE username = ?", (username_clean,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Sales user not found")

    role = normalize_role(row["role"])
    if role not in {"Sales", "Sales Manager", "Admin"}:
        raise HTTPException(status_code=400, detail="Selected user is not a sales user")

    if not bool(row["is_active"]):
        raise HTTPException(status_code=400, detail="Selected sales user is inactive")

    return row["username"], row["full_name"]


@app.get("/sales/users")
def sales_users(user: Dict[str, Any] = Depends(require_roles("Admin", "Sales Manager"))):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT username, full_name, role
        FROM users
        WHERE is_active = 1 AND role IN ('Sales', 'Sales Manager')
        ORDER BY full_name ASC
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return {
        "success": True,
        "users": [
            {"username": row["username"], "fullName": row["full_name"], "role": row["role"]}
            for row in rows
        ],
    }


@app.post("/sales")
def create_sale(data: SalesCreateInput, user: Dict[str, Any] = Depends(require_roles("Admin", "Sales Manager", "Sales"))):
    role = normalize_role(user.get("role", ""))
    sales_username = user.get("username", "")
    sales_full_name = user.get("fullName", "")

    if role in {"Admin", "Sales Manager"} and data.sales_username:
        sales_username, sales_full_name = get_sales_user_display(data.sales_username)

    customer = data.customer_name.strip()
    product = data.product_name.strip()
    if not customer or not product:
        return {"success": False, "message": "Customer name and product name are required"}

    if data.quantity <= 0 or data.unit_price < 0:
        return {"success": False, "message": "Quantity must be greater than 0 and unit price cannot be negative"}

    sale_date = data.sale_date.strip() if data.sale_date else datetime.now().date().isoformat()
    total_amount = round(float(data.quantity) * float(data.unit_price), 2)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sales_transactions
        (sales_username, sales_full_name, sale_date, customer_name, product_name,
         quantity, unit_price, total_amount, payment_method, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sales_username,
            sales_full_name,
            sale_date,
            customer,
            product,
            float(data.quantity),
            float(data.unit_price),
            total_amount,
            data.payment_method.strip(),
            data.notes.strip(),
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    sale_id = cursor.lastrowid
    conn.close()

    return {"success": True, "message": "Sale saved successfully", "saleId": sale_id, "totalAmount": total_amount}


@app.get("/sales/report")
def sales_report(
    sales_username: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    user: Dict[str, Any] = Depends(require_roles("Admin", "Sales Manager", "Sales")),
):
    role = normalize_role(user.get("role", ""))
    params: List[Any] = []
    where = ["1=1"]

    if role == "Sales":
        where.append("sales_username = ?")
        params.append(user.get("username"))
    elif sales_username and sales_username != "all":
        where.append("sales_username = ?")
        params.append(sales_username.strip().lower())

    if from_date:
        where.append("sale_date >= ?")
        params.append(from_date)
    if to_date:
        where.append("sale_date <= ?")
        params.append(to_date)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT *
        FROM sales_transactions
        WHERE {' AND '.join(where)}
        ORDER BY sale_date DESC, id DESC
        """,
        tuple(params),
    )
    rows = cursor.fetchall()
    conn.close()

    return {
        "success": True,
        "sales": sales_rows_to_response(rows),
        "summary": build_sales_summary(rows),
    }


@app.delete("/sales/{sale_id}")
def delete_sale(sale_id: int, user: Dict[str, Any] = Depends(require_roles("Admin", "Sales Manager", "Sales"))):
    role = normalize_role(user.get("role", ""))

    conn = get_db_connection()
    cursor = conn.cursor()
    if role == "Sales":
        cursor.execute("DELETE FROM sales_transactions WHERE id = ? AND sales_username = ?", (sale_id, user.get("username")))
    else:
        cursor.execute("DELETE FROM sales_transactions WHERE id = ?", (sale_id,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()

    if deleted == 0:
        return {"success": False, "message": "Sale not found or you do not have permission to delete it"}

    return {"success": True, "message": "Sale deleted successfully"}




@app.post("/leads")
def create_lead(lead: LeadInput):
    name = lead.name.strip()
    phone = lead.phone.strip()
    job = lead.job.strip()
    request_type = lead.request_type.strip() or "Demo Request"
    source = lead.source.strip() or "Landing Page"

    if not name or not phone:
        return {"success": False, "message": "Name and phone are required"}

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO leads (name, phone, job, request_type, source, status, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            phone,
            job,
            request_type,
            source,
            "New",
            "",
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    lead_id = cursor.lastrowid
    conn.close()

    return {"success": True, "message": "Lead saved successfully", "leadId": lead_id}


@app.get("/admin/leads")
def admin_list_leads(user: Dict[str, Any] = Depends(require_roles("Admin"))):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, phone, job, request_type, source, status, notes, created_at
        FROM leads
        ORDER BY id DESC
        """
    )
    rows = cursor.fetchall()
    conn.close()

    return {
        "success": True,
        "leads": [
            {
                "id": row["id"],
                "name": row["name"],
                "phone": row["phone"],
                "job": row["job"] or "",
                "requestType": row["request_type"] or "",
                "source": row["source"] or "",
                "status": row["status"] or "New",
                "notes": row["notes"] or "",
                "createdAt": row["created_at"],
            }
            for row in rows
        ],
    }


@app.get("/business/status")
def business_status(user: Dict[str, Any] = Depends(get_current_user)):
    settings = get_business_settings()
    settings["activeUsers"] = count_active_users()
    return {"success": True, "business": settings}


@app.get("/admin/business")
def admin_business(user: Dict[str, Any] = Depends(require_roles("Admin"))):
    settings = get_business_settings()
    settings["activeUsers"] = count_active_users()
    return {"success": True, "business": settings}


@app.post("/admin/business/license")
def update_business_license(data: BusinessLicenseInput, user: Dict[str, Any] = Depends(require_roles("Admin"))):
    status = data.license_status.strip().lower()
    if status not in {"trial", "active", "paid", "inactive"}:
        return {"success": False, "message": "License status must be trial, active, paid, or inactive"}

    plan = data.plan.strip() or "Trial"
    max_users = max(1, min(int(data.max_users), 999))
    trial_days = max(0, min(int(data.trial_days), 3650))
    trial_expires_at = (datetime.now() + timedelta(days=trial_days)).isoformat()

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE business_settings
        SET product_name = ?, company_name = ?, plan = ?, license_status = ?,
            trial_expires_at = ?, max_users = ?, license_key_hash = COALESCE(?, license_key_hash), updated_at = ?
        WHERE id = 1
        """,
        (
            data.product_name.strip() or "Reap Service KPI Dashboard",
            data.company_name.strip() or "Demo Company",
            plan,
            status,
            trial_expires_at,
            max_users,
            make_license_hash(data.license_key),
            datetime.now().isoformat(),
        ),
    )
    conn.commit()
    conn.close()

    settings = get_business_settings()
    settings["activeUsers"] = count_active_users()
    return {"success": True, "message": "Business license updated successfully", "business": settings}


@app.post("/login")
def login(data: LoginInput, request: Request):
    username = data.username.strip().lower()
    password = data.password.strip()
    check_login_rate_limit(request, username)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT username, password_hash, role, full_name, is_active
        FROM users
        WHERE username = ?
        """,
        (username,),
    )
    user = cursor.fetchone()
    conn.close()

    if not user:
        record_login_failure(request, username)
        return {"success": False, "message": "Wrong username or password"}

    if not bool(user["is_active"]):
        record_login_failure(request, username)
        return {"success": False, "message": "This user is inactive"}

    if not verify_password(password, user["password_hash"]):
        record_login_failure(request, username)
        return {"success": False, "message": "Wrong username or password"}

    clear_login_failures(request, username)

    safe_role = normalize_role(user["role"])
    token = create_session_token(user["username"], safe_role, user["full_name"])

    return {
        "success": True,
        "token": token,
        "expiresInHours": SESSION_HOURS,
        "business": get_business_settings(),
        "user": {
            "username": user["username"],
            "role": safe_role,
            "fullName": user["full_name"],
        },
    }
@app.get("/odoo/test")
def test_odoo_connection():
    orders = get_sales_orders(limit=5)
    return {
        "message": "Odoo connected successfully",
        "orders_count": len(orders),
        "sample_orders": orders
    }


@app.get("/odoo/sales-kpi")
def odoo_sales_kpi(target: float = 100000):
    return calculate_sales_kpi(target)
from fastapi import UploadFile, File
import shutil
import os


@app.post("/smart-excel/upload")
async def smart_excel_upload(file: UploadFile = File(...)):
    upload_folder = "uploads"
    os.makedirs(upload_folder, exist_ok=True)

    file_path = os.path.join(upload_folder, file.filename)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = analyze_excel_file(file_path)

    return result
