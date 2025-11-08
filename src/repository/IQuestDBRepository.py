from abc import ABC, abstractmethod
from typing import Any


class IQuestDBRepository(ABC):
    """QuestDBとやりとりするためのリポジトリインターフェース"""

    @abstractmethod
    def write(
        self,
        data: list[dict[str, Any]],
        table_name: str,
    ) -> None:
        """データを書き込む"""
        raise NotImplementedError

    @abstractmethod
    def read(
        self,
        query: str,
    ) -> list[dict[str, Any]]:
        """データを読み込む"""
        raise NotImplementedError
