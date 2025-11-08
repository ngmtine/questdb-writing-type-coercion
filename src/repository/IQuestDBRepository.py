from abc import ABC, abstractmethod
from typing import Any, Dict, List


class IQuestDBRepository(ABC):
    """QuestDBとやりとりするためのリポジトリインターフェース"""

    @abstractmethod
    def write(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
    ) -> None:
        """データを書き込む"""
        raise NotImplementedError

    @abstractmethod
    def read(
        self,
        query: str,
    ) -> List[Dict[str, Any]]:
        """データを読み込む"""
        raise NotImplementedError
