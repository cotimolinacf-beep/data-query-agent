"""
BigQuery backend implementation.
Provides read-only access to a specific BigQuery table.
"""
from typing import Optional, TYPE_CHECKING

from backends.base import Backend

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials
    from google.cloud import bigquery


class BigQueryBackend(Backend):
    """BigQuery implementation of the data backend (read-only, single table)."""

    def __init__(
        self,
        credentials: "Credentials",
        project_id: str,
        dataset_id: str,
        table_id: str,
    ):
        self.credentials = credentials
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self._client: Optional["bigquery.Client"] = None

    @property
    def client(self) -> "bigquery.Client":
        """Lazy initialization of BigQuery client."""
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client(
                project=self.project_id,
                credentials=self.credentials,
            )
        return self._client

    @property
    def full_table_id(self) -> str:
        """Return the full table reference: project.dataset.table"""
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def run_query(self, query: str) -> dict:
        """Execute a BigQuery SQL query (SELECT only - enforced upstream)."""
        try:
            query_job = self.client.query(query)
            results = query_job.result()

            columns = [field.name for field in results.schema]
            rows = []
            for row in results:
                # Convert Row to list, handling special types
                row_data = []
                for val in row.values():
                    if val is None:
                        row_data.append(None)
                    elif hasattr(val, 'isoformat'):  # datetime/date
                        row_data.append(val.isoformat())
                    else:
                        row_data.append(val)
                rows.append(row_data)

            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_schema_info(self) -> list[dict]:
        """Get schema for the specific table."""
        try:
            table = self.client.get_table(self.full_table_id)

            col_info = []
            for field in table.schema:
                col_info.append({
                    "name": field.name,
                    "type": field.field_type,
                    "nullable": field.mode != "REQUIRED",
                    "primary_key": False,  # BigQuery doesn't have traditional PKs
                    "sample_values": self._get_sample_values(field.name),
                })

            return [{
                "table_name": self.table_id,
                "row_count": table.num_rows or 0,
                "columns": col_info,
            }]
        except Exception:
            return []

    def _get_sample_values(self, column_name: str, limit: int = 3) -> list[str]:
        """Get sample distinct values for a column."""
        try:
            query = f"""
                SELECT DISTINCT `{column_name}`
                FROM `{self.full_table_id}`
                WHERE `{column_name}` IS NOT NULL
                LIMIT {limit}
            """
            result = self.client.query(query).result()
            samples = []
            for row in result:
                val = row[0]
                if val is not None:
                    if hasattr(val, 'isoformat'):
                        samples.append(val.isoformat())
                    else:
                        samples.append(str(val))
            return samples
        except Exception:
            return []

    def format_schema_for_llm(self) -> str:
        """Format BigQuery schema for LLM context."""
        schema = self.get_schema_info()
        if not schema:
            return f"Table not found: {self.full_table_id}"

        table = schema[0]
        parts = []
        parts.append(f"BigQuery Table: `{self.full_table_id}`")
        parts.append("=" * 60)
        parts.append("")
        parts.append("IMPORTANT: Use this exact table reference in all queries:")
        parts.append(f"  `{self.full_table_id}`")
        parts.append("")

        full_table = f"`{self.full_table_id}`"
        lines = [f"Table: {full_table} ({table['row_count']:,} rows)"]
        lines.append("-" * 50)

        for col in table["columns"]:
            nullable = "NULL" if col["nullable"] else "NOT NULL"
            samples = ", ".join(col["sample_values"]) if col["sample_values"] else "N/A"
            lines.append(
                f"  {col['name']:30s} {col['type']:15s} {nullable}"
            )
            lines.append(f"    Sample values: {samples}")

        parts.append("\n".join(lines))
        parts.append("")

        # Add BigQuery-specific SQL notes
        parts.append("")
        parts.append("== BIGQUERY SQL NOTES ==")
        parts.append(f"- Always use: `{self.full_table_id}` as the table name")
        parts.append("- Date functions: DATE_TRUNC, EXTRACT, FORMAT_DATE")
        parts.append("- Use SAFE_DIVIDE(a, b) to avoid division by zero errors")
        parts.append("- String functions: STARTS_WITH, ENDS_WITH, CONTAINS_SUBSTR")
        parts.append("- Aggregations work like standard SQL")

        return "\n".join(parts)

    def get_column_summary(self, table_name: Optional[str] = None) -> list[dict]:
        """Get summary info for columns (for UI display)."""
        schema = self.get_schema_info()
        if not schema:
            return []

        table = schema[0]
        results = []

        for col in table["columns"]:
            # Get fill percentage
            try:
                query = f"""
                    SELECT
                        COUNT(*) as total,
                        COUNTIF(`{col['name']}` IS NOT NULL) as filled
                    FROM `{self.full_table_id}`
                """
                result = list(self.client.query(query).result())[0]
                total = result.total or 0
                filled = result.filled or 0
                fill_pct = round(filled / total * 100, 1) if total > 0 else 0
            except Exception:
                total = table["row_count"]
                filled = 0
                fill_pct = 0

            if filled == 0:
                continue

            results.append({
                "table": self.table_id,
                "column": col["name"],
                "type": col["type"],
                "filled": filled,
                "total": total,
                "fill_pct": fill_pct,
                "samples": col["sample_values"],
            })

        return results

    def get_tables_list(self) -> list[dict]:
        """List the connected table (single table mode)."""
        schema = self.get_schema_info()
        if not schema:
            return []

        table = schema[0]
        return [{
            "name": self.table_id,
            "rows": table["row_count"],
            "full_id": self.full_table_id,
        }]

    def is_connected(self) -> bool:
        """Check if BigQuery connection is valid and table exists."""
        try:
            # Try to get the specific table
            self.client.get_table(self.full_table_id)
            return True
        except Exception:
            return False

    @property
    def backend_type(self) -> str:
        return "bigquery"
