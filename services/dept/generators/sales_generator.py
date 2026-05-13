import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class SalesGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {"table": "sales", "fields": [{"name": "invoice_number", "type": "string"}]}

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.sales_transactions(days=days))
        self._rows.extend(self.customers())
        self._rows.extend(self.campaigns())
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "customers":
            return self.customers()
        if category == "campaigns":
            return self.campaigns()
        return self.sales_transactions(days=days)

    def sales_transactions(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days * 3):
            gross = round(random.uniform(10000, 200000), 2)
            discount = round(
                gross * float(self.kwargs.get("DISCOUNT_RATE", 0.02)), 2)
            vat_amount = round((gross - discount) * 0.12, 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "invoice_number": f"SALES-{idx+1:05d}",
                "customer_name": fake.company(),
                "transaction_date": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "gross_amount": gross,
                "discount_amount": discount,
                "vat_amount": vat_amount,
                "net_amount": gross - discount + vat_amount,
                "vat_classification": random.choice(["vatable", "exempt", "zero_rated"]),
                "is_interco": random.random() < float(self.kwargs.get("INTERCO_RATE", 0.08)),
            })
        return rows

    def customers(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("CUSTOMER_COUNT", 30))):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "customer_id": f"CUST-{idx+1:04d}",
                "customer_name": fake.company(),
                "tin": fake.bothify(text="###-###-###-###"),
                "address": fake.address(),
                "industry": random.choice(["retail", "manufacturing", "services", "technology"]),
                "credit_limit": round(random.uniform(50000, 250000), 2),
                "is_active": random.random() > 0.1,
                "ingested_at": date.today().isoformat(),
            })
        return rows

    def campaigns(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("CAMPAIGN_COUNT", 8))):
            start = date.today() - timedelta(days=random.randint(0, 90))
            end = start + timedelta(days=random.randint(7, 60))
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "campaign_id": f"CMP-{idx+1:05d}",
                "campaign_name": fake.catch_phrase(),
                "campaign_type": random.choice(["digital", "print", "events", "radio", "tv", "referral"]),
                "channel": random.choice(["Facebook", "Google", "Email", "Event", "TV", "Radio"]),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "budget_amount": round(random.uniform(15000, 200000), 2),
                "actual_spend": round(random.uniform(10000, 180000), 2),
                "leads_generated": random.randint(20, 200),
                "conversions": random.randint(1, 100),
                "revenue_attributed": round(random.uniform(5000, 150000), 2),
                "status": random.choice(["planned", "active", "completed", "cancelled"]),
                "ingested_at": date.today().isoformat(),
            })
        return rows
