"""
SQLite backend implementation.
Wraps the existing db_manager.py functions.
"""
import os
from typing import Optional

from backends.base import Backend
from db_manager import (
    run_query as sqlite_run_query,
    get_schema_info as sqlite_get_schema,
    format_schema_for_llm as sqlite_format_schema,
    get_column_summary as sqlite_get_column_summary,
    load_file_to_sqlite,
    get_db_path,
)


class SQLiteBackend(Backend):
    """SQLite implementation of the data backend."""

    def __init__(self, db_name: str = "data.db"):
        self.db_name = db_name
        self._table_name: Optional[str] = None
        self._file_loaded = False

    def load_file(self, file_path: str, table_name: Optional[str] = None) -> dict:
        """Load a CSV/Excel file into SQLite."""
        result = load_file_to_sqlite(file_path, table_name, self.db_name)
        if result.get("success"):
            self._file_loaded = True
            self._table_name = result.get("table_name")
        return result

    def run_query(self, query: str) -> dict:
        """Execute a SQL query and return results."""
        return sqlite_run_query(query, self.db_name)

    def get_schema_info(self) -> list[dict]:
        """Return schema information for all tables."""
        return sqlite_get_schema(self.db_name)

    def format_schema_for_llm(self) -> str:
        """Format schema as readable string for LLM context."""
        return sqlite_format_schema(self.db_name)

    def get_column_summary(self, table_name: Optional[str] = None) -> list[dict]:
        """Get summary info for columns (for UI display)."""
        tbl = table_name or self._table_name or ""
        return sqlite_get_column_summary(self.db_name, tbl)

    def get_tables_list(self) -> list[dict]:
        """List all tables with row counts."""
        schema = self.get_schema_info()
        return [
            {"name": t["table_name"], "rows": t["row_count"]}
            for t in schema
        ]

    def is_connected(self) -> bool:
        """Check if database exists and has been loaded."""
        db_path = get_db_path(self.db_name)
        return os.path.exists(db_path) and self._file_loaded

    @property
    def backend_type(self) -> str:
        return "sqlite"

    @property
    def table_name(self) -> Optional[str]:
        """Return the name of the loaded table."""
        return self._table_name
