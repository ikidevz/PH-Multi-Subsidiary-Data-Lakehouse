import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class ProcurementGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {"table": "procurement", "fields": [{"name": "vendor_id", "type": "string"}]}

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.vendors())
        self._rows.extend(self.wht_certificates())
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "wht_certificates":
            return self.wht_certificates()
        return self.vendors()

    def vendors(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("VENDOR_COUNT", 25))):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "vendor_id": f"VND-{idx+1:04d}",
                "vendor_name": fake.company(),
                "vendor_tin": fake.bothify(text="###-###-###-###"),
                "vendor_type": random.choice(["goods", "services", "mixed"]),
                "payment_terms": random.choice(["30", "45", "60"]),
                "is_accredited": random.random() < float(self.kwargs.get("ACCREDITED_RATIO", 0.8)),
            })
        return rows

    def purchase_orders(self, days: int = 30) -> list[dict]:
        orders = []
        for idx in range(days * 2):
            income_payment = round(random.uniform(10000, 200000), 2)
            tax_rate = random.choice([0.01, 0.02, 0.05])
            orders.append({
                "subsidiary_id": self.subsidiary_id,
                "po_number": f"PO-{idx+1:05d}",
                "vendor_id": f"VND-{random.randint(1, int(self.kwargs.get('VENDOR_COUNT', 25))):04d}",
                "order_date": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "expected_date": (date.today() + timedelta(days=random.randint(7, 30))).isoformat(),
                "status": random.choice(["open", "approved", "received", "cancelled"]),
                "total_amount": income_payment,
                "wht_amount": round(income_payment * tax_rate, 2),
                "atc_code": random.choice(["WC010", "WC030", "WC155"]),
                "accredited_vendor": random.random() < float(self.kwargs.get("ACCREDITED_RATIO", 0.8)),
            })
        return orders

    def wht_certificates(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("VENDOR_COUNT", 25))):
            income_payment = round(random.uniform(1000, 120000), 2)
            tax_rate = random.choice([0.01, 0.02, 0.05])
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "cert_number": f"2307-{date.today().year}-{self.subsidiary_id}-{idx+1:04d}",
                "vendor_id": f"VND-{idx+1:04d}",
                "atc_code": random.choice(["WC010", "WC030", "WC155"]),
                "income_payment": income_payment,
                "tax_rate": tax_rate,
                "wht_amount": round(income_payment * tax_rate, 2),
                "issued_date": (date.today() - timedelta(days=random.randint(0, 30))).isoformat(),
                "issued_to": fake.company(),
            })
        return rows

    def vendor_scorecard(self, month: int = 0, year: int | None = None) -> list[dict]:
        if year is None:
            year = date.today().year
        rows = []
        for idx in range(int(self.kwargs.get("VENDOR_COUNT", 25))):
            on_time = round(random.uniform(0.7, 0.98), 2)
            fill_rate = round(random.uniform(0.75, 0.99), 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "vendor_id": f"VND-{idx+1:04d}",
                "month": month,
                "year": year,
                "on_time_delivery_rate": on_time,
                "po_fill_rate": fill_rate,
                "payment_compliance": round(random.uniform(0.8, 0.99), 2),
                "price_variance": round(random.uniform(-0.05, 0.05), 4),
            })
        return rows

    def ap_aging_detail(self, as_of_date: str | None = None) -> list[dict]:
        if as_of_date is None:
            as_of_date = date.today().isoformat()
        rows = []
        for idx in range(int(self.kwargs.get("VENDOR_COUNT", 25))):
            current = round(random.uniform(10000, 50000), 2)
            bucket_31_60 = round(random.uniform(5000, 25000), 2)
            bucket_61_90 = round(random.uniform(2000, 15000), 2)
            bucket_over_90 = round(random.uniform(1000, 10000), 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "vendor_id": f"VND-{idx+1:04d}",
                "as_of_date": as_of_date,
                "current": current,
                "aging_31_60": bucket_31_60,
                "aging_61_90": bucket_61_90,
                "aging_over_90": bucket_over_90,
                "total_outstanding": round(
                    current + bucket_31_60 + bucket_61_90 + bucket_over_90, 2
                ),
            })
        return rows
