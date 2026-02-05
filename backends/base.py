"""
Abstract base class for data backends.
"""
from abc import ABC, abstractmethod
from typing import Optional


class Backend(ABC):
    """Abstract base class for data backends (SQLite, BigQuery, etc.)."""

    @abstractmethod
    def run_query(self, query: str) -> dict:
        """
        Execute a SQL query and return results.

        Returns:
            dict with keys:
                - success: bool
                - columns: list[str] (if success)
                - rows: list[list] (if success)
                - row_count: int (if success)
                - error: str (if not success)
        """
        pass

    @abstractmethod
    def get_schema_info(self) -> list[dict]:
        """
        Return schema information for all tables.

        Returns:
            list of dicts with keys:
                - table_name: str
                - row_count: int
                - columns: list[dict] with name, type, nullable, sample_values
        """
        pass

    @abstractmethod
    def format_schema_for_llm(self) -> str:
        """
        Format schema as a readable string for LLM context.

        Returns:
            Formatted string with table names, columns, types, and sample values.
        """
        pass

    @abstractmethod
    def get_column_summary(self, table_name: Optional[str] = None) -> list[dict]:
        """
        Get summary info for columns (for UI display).

        Returns:
            list of dicts with: table, column, type, fill_pct, samples
        """
        pass

    @abstractmethod
    def get_tables_list(self) -> list[dict]:
        """
        List all tables with row counts.

        Returns:
            list of dicts with: name, rows
        """
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if backend is ready for queries."""
        pass

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Return backend type identifier: 'sqlite' or 'bigquery'."""
        pass
