import pandas as pd
from difflib import get_close_matches


COLUMN_MAP = {
    "date": [
        "date", "invoice date", "order date", "transaction date",
        "تاريخ", "التاريخ", "تاريخ الفاتورة"
    ],
    "amount": [
        "amount", "total", "sales", "revenue", "net sales", "value",
        "الإجمالي", "المبلغ", "قيمة البيع", "اجمالي"
    ],
    "customer": [
        "customer", "client", "partner", "customer name",
        "العميل", "اسم العميل"
    ],
    "sales_rep": [
        "sales rep", "salesperson", "seller", "employee",
        "مندوب", "البائع", "الموظف"
    ],
    "product": [
        "product", "item", "sku", "product name",
        "الصنف", "المنتج", "اسم الصنف"
    ],
    "quantity": [
        "quantity", "qty", "units",
        "الكمية", "عدد"
    ],
    "expense": [
        "expense", "cost", "expenses",
        "مصروف", "المصروفات", "تكلفة"
    ],
    "profit": [
        "profit", "net profit", "margin",
        "ربح", "صافي الربح", "هامش الربح"
    ]
}


def normalize_text(text):
    return str(text).strip().lower().replace("_", " ")


def detect_columns(excel_columns):
    detected = {}
    normalized_columns = {
        normalize_text(col): col for col in excel_columns
    }

    for standard_name, possible_names in COLUMN_MAP.items():
        possible_names_clean = [normalize_text(x) for x in possible_names]

        for clean_col, original_col in normalized_columns.items():
            if clean_col in possible_names_clean:
                detected[standard_name] = original_col
                break

            match = get_close_matches(
                clean_col,
                possible_names_clean,
                n=1,
                cutoff=0.75
            )

            if match:
                detected[standard_name] = original_col
                break

    return detected


def detect_file_type(detected_columns):
    keys = set(detected_columns.keys())

    if {"date", "amount", "customer"}.issubset(keys) or {"amount", "sales_rep"}.issubset(keys):
        return "Sales"

    if {"expense", "profit"}.intersection(keys) or {"amount", "expense"}.issubset(keys):
        return "Finance"

    if {"product", "quantity"}.issubset(keys):
        return "Inventory"

    return "Unknown"


def analyze_excel_file(file_path):
    df = pd.read_excel(file_path)

    detected = detect_columns(df.columns)
    file_type = detect_file_type(detected)

    result = {
        "file_type": file_type,
        "detected_columns": detected,
        "rows_count": len(df),
        "columns_count": len(df.columns),
        "original_columns": list(df.columns)
    }

    if file_type == "Sales" and "amount" in detected:
        amount_col = detected["amount"]
        df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

        result["analysis"] = {
            "total_sales": round(df[amount_col].sum(), 2),
            "average_sales": round(df[amount_col].mean(), 2),
            "max_sale": round(df[amount_col].max(), 2),
            "min_sale": round(df[amount_col].min(), 2)
        }

    elif file_type == "Inventory" and "quantity" in detected:
        qty_col = detected["quantity"]
        df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

        result["analysis"] = {
            "total_quantity": round(df[qty_col].sum(), 2),
            "average_quantity": round(df[qty_col].mean(), 2),
            "low_stock_items": int((df[qty_col] <= 5).sum())
        }

    elif file_type == "Finance":
        result["analysis"] = {
            "message": "Finance file detected. Finance analysis can be expanded."
        }

    else:
        result["analysis"] = {
            "message": "File type not detected clearly."
        }

    return result