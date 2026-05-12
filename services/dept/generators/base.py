from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from faker import Faker

fake = Faker("en_PH")


class DepartmentGenerator(ABC):
    def __init__(self, subsidiary_id: str, **kwargs: Any) -> None:
        self.subsidiary_id = subsidiary_id
        self.kwargs = kwargs
        self._rows = []

    @abstractmethod
    def schema(self) -> dict:
        pass

    @abstractmethod
    def generate_all(self, days: int = 30) -> int:
        pass

    @abstractmethod
    def records(self, days: int = 30, category: str | None = None) -> list[dict]:
        pass

    def metrics(self) -> dict:
        return {
            "total_rows_generated": len(self._rows),
            "last_generated_at": date.today().isoformat() if self._rows else None,
            "subsidiary_id": self.subsidiary_id,
        }

    def reset(self) -> None:
        self._rows = []
