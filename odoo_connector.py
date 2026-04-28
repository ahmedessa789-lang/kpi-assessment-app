import os
import xmlrpc.client
from dotenv import load_dotenv

load_dotenv()

ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")


def get_odoo_client():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})

    if not uid:
        raise Exception("Odoo authentication failed. Check URL, DB, username, or password.")

    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    return uid, models


def get_sales_orders(limit=50):
    uid, models = get_odoo_client()

    orders = models.execute_kw(
        ODOO_DB,
        uid,
        ODOO_PASSWORD,
        "sale.order",
        "search_read",
        [[["state", "in", ["sale", "done"]]]],
        {
            "fields": [
                "name",
                "date_order",
                "partner_id",
                "user_id",
                "amount_total",
                "state"
            ],
            "limit": limit,
            "order": "date_order desc"
        }
    )

    return orders


def calculate_sales_kpi(target=100000):
    orders = get_sales_orders()

    total_sales = sum(order.get("amount_total", 0) for order in orders)
    total_orders = len(orders)
    average_order = total_sales / total_orders if total_orders else 0
    achievement = (total_sales / target) * 100 if target else 0

    if achievement >= 100:
        status = "Excellent"
    elif achievement >= 75:
        status = "Good"
    elif achievement >= 50:
        status = "Average"
    else:
        status = "Needs Improvement"

    return {
        "target": target,
        "actual_sales": round(total_sales, 2),
        "total_orders": total_orders,
        "average_order_value": round(average_order, 2),
        "achievement_percent": round(achievement, 2),
        "status": status,
        "orders": orders
    }