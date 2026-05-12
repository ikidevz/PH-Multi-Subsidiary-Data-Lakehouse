import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class LegalGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {"table": "legal", "fields": [{"name": "filing_type", "type": "string"}]}

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.sec_filings())
        self._rows.extend(self.stockholders())
        self._rows.extend(self.board_resolutions())
        self._rows.extend(self.officers())
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "stockholders":
            return self.stockholders()
        if category == "board_resolutions":
            return self.board_resolutions()
        if category == "officers":
            return self.officers()
        return self.sec_filings()

    def sec_filings(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("STOCKHOLDER_COUNT", 5))):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "filing_type": random.choice(["GIS", "AFS", "ACGR"]),
                "due_date": (date.today() + timedelta(days=30)).isoformat(),
                "filing_date": (date.today() + timedelta(days=random.randint(0, 60))).isoformat(),
                "status": random.choice(["filed", "late", "pending"]),
                "reference_number": f"SEC-{random.randint(100000, 999999)}",
            })
        return rows

    def stockholders(self) -> list[dict]:
        rows = []
        total_ownership = 0
        count = int(self.kwargs.get("STOCKHOLDER_COUNT", 5))
        for idx in range(count):
            pct = round(100 / count, 2)
            total_ownership += pct
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "stockholder_id": f"SH-{idx+1:04d}",
                "name": fake.name(),
                "ownership_pct": pct,
                "is_foreign": random.random() < 0.3,
            })
        if total_ownership != 100:
            rows[-1]["ownership_pct"] += 100 - total_ownership
        return rows

    def board_resolutions(self) -> list[dict]:
        rows = []
        year = date.today().year
        for idx in range(1, int(self.kwargs.get("RESOLUTION_COUNT", 5)) + 1):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "resolution_number": f"BR-{year}-{idx:03d}",
                "resolution_date": (date.today() - timedelta(days=idx * 10)).isoformat(),
                "title": fake.sentence(nb_words=6),
                "enacted_by": fake.name(),
            })
        return rows

    def officers(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("OFFICER_COUNT", 6))):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "officer_id": f"OFF-{idx+1:04d}",
                "name": fake.name(),
                "position": random.choice(["CEO", "CFO", "COO", "General Counsel", "Corporate Secretary"]),
                "start_date": (date.today() - timedelta(days=random.randint(30, 3650))).isoformat(),
                "end_date": (
                    None if random.random() < 0.8
                    else (date.today() - timedelta(days=random.randint(0, 365))).isoformat()
                ),
            })
        return rows
