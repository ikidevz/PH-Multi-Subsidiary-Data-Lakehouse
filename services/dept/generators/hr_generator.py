import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class HRGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {"table": "hr", "fields": [{"name": "employee_id", "type": "string"}]}

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.employees())
        self._rows.extend(self.payroll_runs(days=days))
        self._rows.extend(self.statutory_contributions(days=days))
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "payroll_runs":
            return self.payroll_runs(days=days)
        if category == "statutory_contributions":
            return self.statutory_contributions(days=days)
        return self.employees()

    def employees(self) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("HEADCOUNT", 50))):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "employee_id": f"EMP-{idx+1:04d}",
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "tin": fake.bothify(text="###-###-###-###"),
                "sss_number": fake.bothify(text="##-#######-#"),
                "philhealth_number": fake.numerify(text="############"),
                "pagibig_number": fake.numerify(text="############"),
                "department": random.choice(["Finance", "Operations", "Sales", "HR"]),
                "position": random.choice(["Staff", "Supervisor", "Manager"]),
                "employment_type": random.choice(["regular", "contractual"]),
                "basic_salary": round(
                    random.uniform(
                        float(self.kwargs.get("MIN_SALARY", 15000)),
                        float(self.kwargs.get("MAX_SALARY", 250000)),
                    ),
                    2,
                ),
            })
        return rows

    def payroll_runs(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(int(self.kwargs.get("HEADCOUNT", 50))):
            gross_pay = round(random.uniform(20000, 150000), 2)
            social = round(gross_pay * 0.08, 2)
            health = round(gross_pay * 0.03, 2)
            housing = round(gross_pay * 0.02, 2)
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "payroll_period": f"{date.today().year}-{(date.today().month - 1):02d}",
                "employee_id": f"EMP-{idx+1:04d}",
                "gross_pay": gross_pay,
                "total_deductions": social + health + housing,
                "net_pay": gross_pay - social - health - housing,
                "payroll_date": (date.today() - timedelta(days=random.randint(0, 30))).isoformat(),
            })
        return rows

    def statutory_contributions(self, days: int = 30) -> list[dict]:
        rows = []
        for month in range(1, 4):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "period_month": month,
                "period_year": date.today().year,
                "sss_employer": round(random.uniform(50000, 120000), 2),
                "sss_employee": round(random.uniform(25000, 60000), 2),
                "philhealth_employer": round(random.uniform(7000, 18000), 2),
                "philhealth_employee": round(random.uniform(3500, 9000), 2),
                "pagibig_employer": round(random.uniform(2000, 6500), 2),
                "pagibig_employee": round(random.uniform(1000, 3500), 2),
                "total_amount": 0,
            })
        for row in rows:
            row["total_amount"] = round(
                row["sss_employer"] + row["sss_employee"]
                + row["philhealth_employer"] + row["philhealth_employee"]
                + row["pagibig_employer"] + row["pagibig_employee"],
                2,
            )
        return rows

    def dole_report(self, year: int | None = None) -> list[dict]:
        if year is None:
            year = date.today().year
        total_employees = int(self.kwargs.get("HEADCOUNT", 50))
        regular_count = int(total_employees * 0.7)
        contractual_count = total_employees - regular_count
        male_pct = round(random.uniform(0.39, 0.61), 2)
        female_pct = round(1.0 - male_pct, 2)
        return [{
            "subsidiary_id": self.subsidiary_id,
            "year": year,
            "total_employees": total_employees,
            "regular_count": regular_count,
            "contractual_count": contractual_count,
            "male_pct": male_pct,
            "female_pct": female_pct,
            "avg_monthly_salary": round(random.uniform(25000, 100000), 2),
            "total_wages": round(total_employees * random.uniform(25000, 100000), 2),
            "report_date": date.today().isoformat(),
        }]
