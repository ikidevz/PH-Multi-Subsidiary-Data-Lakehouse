import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class OpsGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {"table": "ops", "fields": [{"name": "movement_type", "type": "string"}]}

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.inventory_movements(days=days))
        self._rows.extend(self.purchase_orders(days=days))
        self._rows.extend(self.sales_orders(days=days))
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "purchase_orders":
            return self.purchase_orders(days=days)
        if category == "sales_orders":
            return self.sales_orders(days=days)
        return self.inventory_movements(days=days)

    def inventory_movements(self, days: int = 30) -> list[dict]:
        movements = []
        for idx in range(days * 3):
            movement_type = random.choice(["receipt", "issue", "transfer", "adjustment"])
            qty = random.randint(1, 100)
            movements.append({
                "subsidiary_id": self.subsidiary_id,
                "movement_type": movement_type,
                "item_code": f"SKU-{random.randint(100, 999)}",
                "quantity": qty,
                "unit_cost": round(random.uniform(50, 1500), 2),
                "total_cost": round(qty * random.uniform(50, 1500), 2),
                "warehouse_location": f"WH-{random.randint(1, int(self.kwargs.get('WAREHOUSE_COUNT', 2)))}",
                "reference_doc": fake.bothify(text="PO-####" if movement_type == "receipt" else "SO-####"),
            })
        return movements

    def purchase_orders(self, days: int = 30) -> list[dict]:
        orders = []
        for idx in range(days * 2):
            orders.append({
                "subsidiary_id": self.subsidiary_id,
                "po_number": f"PO-{idx+1:05d}",
                "vendor_id": f"VND-{random.randint(1, 100):04d}",
                "order_date": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "expected_date": (date.today() + timedelta(days=random.randint(7, 30))).isoformat(),
                "status": random.choice(["open", "approved", "received", "cancelled"]),
                "total_amount": round(random.uniform(10000, 200000), 2),
            })
        return orders

    def sales_orders(self, days: int = 30) -> list[dict]:
        orders = []
        for idx in range(days * 2):
            orders.append({
                "subsidiary_id": self.subsidiary_id,
                "so_number": f"SO-{idx+1:05d}",
                "customer_id": f"CUST-{random.randint(1, 100):04d}",
                "order_date": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "ship_date": (date.today() + timedelta(days=random.randint(1, 15))).isoformat(),
                "status": random.choice(["pending", "shipped", "completed", "returned"]),
                "total_amount": round(random.uniform(15000, 250000), 2),
            })
        return orders

    def stock_summary(self, as_of_date: str | None = None) -> list[dict]:
        if as_of_date is None:
            as_of_date = date.today().isoformat()
        rows = []
        for idx in range(1, int(self.kwargs.get("PRODUCT_COUNT", 50)) + 1):
            qty = random.randint(0, 500)
            unit_cost = round(random.uniform(50, 1500), 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "item_code": f"SKU-{idx:04d}",
                "warehouse_location": f"WH-{random.randint(1, int(self.kwargs.get('WAREHOUSE_COUNT', 2)))}",
                "quantity_on_hand": qty,
                "unit_cost": unit_cost,
                "total_value": round(qty * unit_cost, 2),
                "days_of_stock": round(qty / max(1, random.randint(1, 20)), 2),
                "as_of_date": as_of_date,
            })
        return rows
