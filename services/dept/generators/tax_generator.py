import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class TaxGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {
            "table": "tax",
            "fields": [
                {"name": "form_type", "type": "string"},
                {"name": "gross_sales", "type": "number"},
                {"name": "vat_payable", "type": "number"},
            ],
        }

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.vat_returns())
        self._rows.extend(self.wht_filings())
        self._rows.extend(self.bir_filings_log())
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "wht_filings":
            return self.wht_filings()
        if category == "bir_filings_log":
            return self.bir_filings_log()
        if category == "alphalist":
            return self.alphalist()
        return self._rows or self.vat_returns()

    def vat_returns(self, months: int = 3) -> list[dict]:
        rows = []
        for idx in range(months):
            gross = round(random.uniform(500000, 2500000), 2)
            output_vat = round(gross * float(self.kwargs.get("VAT_RATE", 0.12)), 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "form_type": random.choice(["2550M", "2550Q"]),
                "period_month": idx + 1,
                "period_year": date.today().year,
                "gross_sales": gross,
                "taxable_sales": gross * 0.9,
                "output_vat": output_vat,
                "input_vat": round(gross * 0.08, 2),
                "vat_payable": round(output_vat - gross * 0.08, 2),
                "status": random.choice(["pending", "filed", "late"]),
            })
        return rows

    def wht_filings(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days * 2):
            income_payment = round(random.uniform(5000, 120000), 2)
            tax_rate = random.choice([0.01, 0.02, 0.05])
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "filing_id": f"WHT-{idx+1:05d}",
                "taxpayer_name": fake.company(),
                "income_payment": income_payment,
                "tax_rate": tax_rate,
                "wht_amount": round(income_payment * tax_rate, 2),
                "form_type": random.choice(["1601C", "1601E", "2307"]),
                "atc_code": random.choice(["WC010", "WC030", "WC155"]),
                "issued_date": (date.today() - timedelta(days=random.randint(0, 30))).isoformat(),
            })
        return rows

    def bir_filings_log(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "record_id": f"BIR-{idx+1:05d}",
                "filing_type": random.choice(["2550M", "2550Q", "1601C"]),
                "status": random.choice(["accepted", "rejected", "pending"]),
                "submitted_at": (date.today() - timedelta(days=random.randint(0, 7))).isoformat(),
                "efps_reference": f"EFPS-{random.randint(10000, 99999)}",
            })
        return rows

    def alphalist(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("ATC_ROW_COUNT", 10))):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "payee_name": fake.company(),
                "tin": fake.bothify(text="###-###-###-###"),
                "atc_code": random.choice(["WC010", "WC155", "WC030"]),
                "income_payment": round(random.uniform(20000, 180000), 2),
                "wht_withheld": round(random.uniform(200, 1800), 2),
                "year": date.today().year,
            })
        return rows
