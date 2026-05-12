import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class FinanceGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {
            "table": "finance",
            "fields": [
                {"name": "entry_number", "type": "string"},
                {"name": "account_code", "type": "string"},
                {"name": "debit_amount", "type": "number"},
                {"name": "credit_amount", "type": "number"},
            ],
        }

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.journal_entries(days=days))
        self._rows.extend(self.ap_invoices(days=days))
        self._rows.extend(self.ar_ledger(days=days))
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "ap_invoices":
            return self.ap_invoices(days=days)
        if category == "ar_ledger":
            return self.ar_ledger(days=days)
        return self.journal_entries(days=days)

    def journal_entries(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days * int(self.kwargs.get("AVG_DAILY_JE_COUNT", 5))):
            debit = round(random.uniform(1000, 150000), 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "entry_date": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "entry_number": f"JE-{idx+1:05d}",
                "account_code": random.choice(["1010", "2000", "3000", "5000"]),
                "debit_amount": debit,
                "credit_amount": debit,
                "description": fake.sentence(nb_words=6),
                "is_interco": random.random() < float(self.kwargs.get("INTERCO_RATE", 0.1)),
            })
        return rows

    def ap_invoices(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days * 2):
            gross = round(random.uniform(10000, 150000), 2)
            vat = round(gross * float(self.kwargs.get("VAT_RATE", 0.12)), 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "invoice_number": f"AP-{idx+1:05d}",
                "vendor_name": fake.company(),
                "invoice_date": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "due_date": (date.today() + timedelta(days=int(self.kwargs.get("PAYMENT_TERMS_DAYS", 30)))).isoformat(),
                "gross_amount": gross,
                "vat_amount": vat,
                "net_amount": gross - vat,
                "wht_amount": round(gross * float(self.kwargs.get("WHT_RATE_DEFAULT", 0.01)), 2),
                "status": random.choice(["unpaid", "paid", "overdue"]),
            })
        return rows

    def ar_ledger(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days * 2):
            gross = round(random.uniform(10000, 250000), 2)
            vat = round(gross * 0.12, 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "invoice_number": f"AR-{idx+1:05d}",
                "customer_name": fake.company(),
                "invoice_date": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "due_date": (date.today() + timedelta(days=30)).isoformat(),
                "gross_amount": gross,
                "vat_amount": vat,
                "net_amount": gross - vat,
                "collected_amount": round(gross * random.uniform(0.2, 1.0), 2),
                "status": random.choice(["open", "collected", "overdue"]),
            })
        return rows

    def trial_balance(self, days: int = 30) -> list[dict]:
        accounts = ["1000-Cash", "2000-AR", "3000-AP", "4000-Revenue", "5000-COGS", "6000-Expense"]
        balances = []
        for account in accounts:
            debit = round(random.uniform(50000, 250000), 2)
            credit = round(random.uniform(50000, 250000), 2)
            balances.append({
                "subsidiary_id": self.subsidiary_id,
                "account_code": account,
                "debit_balance": debit,
                "credit_balance": credit,
                "as_of_date": date.today().isoformat(),
            })
        return balances
