import random
from datetime import date, timedelta

from .base import DepartmentGenerator, fake


class ITAuditGenerator(DepartmentGenerator):
    def schema(self) -> dict:
        return {"table": "it_audit", "fields": [{"name": "event_type", "type": "string"}]}

    def generate_all(self, days: int = 30) -> int:
        self._rows = []
        self._rows.extend(self.audit_log(days=days))
        self._rows.extend(self.access_events(days=days))
        self._rows.extend(self.system_incidents(days=days))
        return len(self._rows)

    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        if category == "access_events":
            return self.access_events(days=days)
        if category == "system_incidents":
            return self.system_incidents(days=days)
        return self.audit_log(days=days)

    def audit_log(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days * 4):
            success = random.random() > float(self.kwargs.get("FAILED_LOGIN_RATE", 0.03))
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "event_timestamp": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "user_id": f"USER-{random.randint(1, int(self.kwargs.get('USER_COUNT', 60)))}",
                "action": random.choice(["LOGIN", "LOGOUT", "READ", "EXPORT", "DELETE"]),
                "result": "success" if success else "failed",
                "risk_level": random.choice(["low", "medium", "high", "critical"]),
            })
        return rows

    def access_events(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days * 2):
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "event_timestamp": (date.today() - timedelta(days=random.randint(0, days))).isoformat(),
                "user_id": f"USER-{random.randint(1, int(self.kwargs.get('USER_COUNT', 60)))}",
                "action": random.choice(["READ", "WRITE", "DELETE", "EXPORT"]),
                "resource": random.choice(["payroll", "journal_entries", "ap_invoices", "employees"]),
                "resource_id": f"RES-{random.randint(1000, 9999)}",
                "ip_address": fake.ipv4(),
                "result": random.choice(["allowed", "denied"]),
            })
        return rows

    def system_incidents(self, days: int = 30) -> list[dict]:
        rows = []
        for idx in range(days):
            incident_date = date.today() - timedelta(days=random.randint(0, days))
            resolved = random.random() < 0.75
            rows.append({
                "subsidiary_id": self.subsidiary_id,
                "incident_id": f"INC-{idx+1:05d}",
                "incident_date": incident_date.isoformat(),
                "incident_type": random.choice(["outage", "security_breach", "data_loss", "performance"]),
                "severity": random.choice(["low", "medium", "high", "critical"]),
                "description": fake.sentence(nb_words=8),
                "resolved_date": (
                    (incident_date + timedelta(days=random.randint(1, 7))).isoformat()
                    if resolved else None
                ),
            })
        return rows
