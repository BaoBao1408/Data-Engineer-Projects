from sqlalchemy import text
from db.connection import engine
from utils.logger import log


def update_order_totals():
    """
    Update orders.total_amount based on order_item subtotal.
    Must be run AFTER order_item generation is completed.
    """

    log("Updating orders.total_amount...")

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE orders o
            SET total_amount = s.total
            FROM (
                SELECT 
                    oi.order_id, 
                    SUM(oi.subtotal) AS total
                FROM order_item oi                
                GROUP BY oi.order_id
            ) s
            WHERE o.order_id = s.order_id
                AND (o.total_amount IS NULL OR o.total_amount = 0);
        """))

    log("orders.total_amount updated successfully")
