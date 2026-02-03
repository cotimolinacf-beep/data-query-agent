"""
Database manager: loads CSV/XLS files into SQLite and provides schema info.
"""
import os
import re
import sqlite3
import pandas as pd
from typing import Optional


DB_DIR = os.path.join(os.path.dirname(__file__), "databases")
os.makedirs(DB_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Spanish date parsing
# ---------------------------------------------------------------------------
_SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

_SPANISH_DATE_RE = re.compile(
    r'(\w+)\s+(\d{1,2}),\s*(\d{4}),?\s*(\d{1,2}):(\d{2})\s*(a\.?\s*m\.?|p\.?\s*m\.?)',
    re.IGNORECASE,
)


def _parse_spanish_date(value: str) -> Optional[str]:
    """Convert 'diciembre 22, 2025, 4:13 p. m.' → '2025-12-22 16:13:00'.
    Returns None if the value doesn't match the expected format."""
    if not isinstance(value, str):
        return None
    # Normalize non-breaking spaces and extra whitespace
    cleaned = value.replace('\xa0', ' ').strip()
    m = _SPANISH_DATE_RE.match(cleaned)
    if not m:
        return None
    month_name = m.group(1).lower()
    month = _SPANISH_MONTHS.get(month_name)
    if month is None:
        return None
    day = int(m.group(2))
    year = int(m.group(3))
    hour = int(m.group(4))
    minute = int(m.group(5))
    ampm = m.group(6).replace('.', '').replace(' ', '').lower()
    if ampm == 'pm' and hour != 12:
        hour += 12
    elif ampm == 'am' and hour == 12:
        hour = 0
    return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00"


def _convert_spanish_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Detect and convert columns containing Spanish-format dates to ISO."""
    for col in df.columns:
        if df[col].dtype != object:
            continue
        sample = df[col].dropna().head(10)
        if len(sample) == 0:
            continue
        converted = sample.apply(_parse_spanish_date)
        hit_rate = converted.notna().sum() / len(sample)
        if hit_rate >= 0.7:
            df[col] = df[col].apply(
                lambda v: _parse_spanish_date(v) if isinstance(v, str) else v
            )
    return df


def get_db_path(db_name: str = "data.db") -> str:
    return os.path.join(DB_DIR, db_name)


def load_file_to_sqlite(
    file_path: str,
    table_name: Optional[str] = None,
    db_name: str = "data.db",
) -> dict:
    """
    Load a CSV or XLS/XLSX file into a SQLite database.
    Returns a dict with status info.
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(file_path)
    elif ext in (".xls", ".xlsx"):
        df = pd.read_excel(file_path, engine="openpyxl")
    else:
        return {"success": False, "error": f"Unsupported file type: {ext}"}

    if table_name is None:
        table_name = (
            os.path.splitext(os.path.basename(file_path))[0]
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )

    # Sanitize column names for SQL compatibility
    df.columns = [
        col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    ]

    # Auto-convert Spanish date columns to ISO format
    df = _convert_spanish_date_columns(df)

    db_path = get_db_path(db_name)
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        row_count = len(df)
    finally:
        conn.close()

    return {
        "success": True,
        "db_path": db_path,
        "table_name": table_name,
        "columns": list(df.columns),
        "row_count": row_count,
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }


def get_column_summary(
    db_name: str = "data.db",
    table_name: str = "",
) -> list[dict]:
    """Return a compact summary of each column for the UI description form.
    Only includes columns that have at least one non-null value.
    If table_name is given, only that table is inspected."""
    db_path = get_db_path(db_name)
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    results = []
    try:
        if table_name:
            tables = [table_name]
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [r[0] for r in cursor.fetchall()]
        for table in tables:
            cursor.execute(f"PRAGMA table_info('{table}');")
            columns = cursor.fetchall()
            for col in columns:
                col_name = col[1]
                col_type = col[2] or "TEXT"
                # Count non-null, non-empty values
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{table}" '
                    f'WHERE "{col_name}" IS NOT NULL AND TRIM("{col_name}") != \'\''
                )
                filled = cursor.fetchone()[0]
                if filled == 0:
                    continue
                # Get sample distinct values
                cursor.execute(
                    f'SELECT DISTINCT "{col_name}" FROM "{table}" '
                    f'WHERE "{col_name}" IS NOT NULL AND TRIM("{col_name}") != \'\' '
                    f'LIMIT 5'
                )
                samples = [str(r[0]) for r in cursor.fetchall()]
                # Total rows
                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                total = cursor.fetchone()[0]
                results.append({
                    "table": table,
                    "column": col_name,
                    "type": col_type,
                    "filled": filled,
                    "total": total,
                    "fill_pct": round(filled / total * 100, 1) if total else 0,
                    "samples": samples,
                })
    finally:
        conn.close()
    return results


def get_schema_info(db_name: str = "data.db") -> list[dict]:
    """Return schema information for all tables in the database."""
    db_path = get_db_path(db_name)
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

        schema = []
        for table in tables:
            cursor.execute(f"PRAGMA table_info('{table}');")
            columns = cursor.fetchall()

            cursor.execute(f"SELECT COUNT(*) FROM '{table}';")
            row_count = cursor.fetchone()[0]

            # Get sample values for each column
            cursor.execute(f"SELECT * FROM '{table}' LIMIT 3;")
            sample_rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            col_info = []
            for col in columns:
                col_idx = col[0]
                samples = [
                    str(row[col_idx])
                    for row in sample_rows
                    if row[col_idx] is not None
                ]
                col_info.append({
                    "name": col[1],
                    "type": col[2],
                    "nullable": not col[3],
                    "primary_key": bool(col[5]),
                    "sample_values": samples[:3],
                })

            schema.append({
                "table_name": table,
                "row_count": row_count,
                "columns": col_info,
            })

        return schema
    finally:
        conn.close()


def run_query(query: str, db_name: str = "data.db") -> dict:
    """Execute a SQL query and return results."""
    db_path = get_db_path(db_name)
    if not os.path.exists(db_path):
        return {"success": False, "error": "Database not found."}

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
            }
        else:
            conn.commit()
            return {"success": True, "message": "Query executed (no results)."}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def _looks_numeric(value: str) -> bool:
    """Return True if *value* looks like a formatted number, price, or range.
    Rejects dates (YYYY-MM-DD) and strings that are mostly alphabetic."""
    # Reject ISO dates
    if re.match(r'^\d{4}-\d{2}-\d{2}', value):
        return False
    # Strip known prefixes/symbols
    cleaned = re.sub(r'[Dd]esde\s*|[Ff]rom\s*|[$＄€£¥,\s]', '', value)
    # For ranges like "X - Y", check the first part
    first_part = re.split(r'\s*-\s*', cleaned)[0]
    # Must be mostly digits and punctuation (.,), not letters
    if not first_part:
        return False
    alpha_count = sum(1 for c in first_part if c.isalpha())
    digit_count = sum(1 for c in first_part if c.isdigit())
    return digit_count > 0 and alpha_count <= digit_count * 0.3


def _detect_numeric_text_columns(db_name: str = "data.db") -> list[dict]:
    """
    For every TEXT column that stores numeric-looking data, inspect all distinct
    values and build a concrete SQLite expression to extract a clean REAL number.
    """
    db_path = get_db_path(db_name)
    if not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    results = []

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cursor.fetchall()]

        for table in tables:
            cursor.execute(f"PRAGMA table_info('{table}');")
            columns = cursor.fetchall()

            for col_info in columns:
                col_name = col_info[1]
                col_type = (col_info[2] or "").upper()
                if col_type != "TEXT":
                    continue

                cursor.execute(
                    f'SELECT DISTINCT "{col_name}" FROM "{table}" '
                    f'WHERE "{col_name}" IS NOT NULL;'
                )
                distinct_vals = [str(r[0]) for r in cursor.fetchall() if r[0]]

                if not distinct_vals:
                    continue

                numeric_count = sum(1 for v in distinct_vals if _looks_numeric(v))
                if numeric_count < len(distinct_vals) * 0.5:
                    continue

                # --- Detect patterns ---
                prefixes_found = set()
                symbols_found = set()
                has_range_spaced = False
                has_range_nospace = False

                for v in distinct_vals:
                    m = re.match(r'^(Desde|From|desde|from)\s+', v)
                    if m:
                        prefixes_found.add(m.group(0))
                    for sym in ['＄', '$', '€', '£', '¥']:
                        if sym in v:
                            symbols_found.add(sym)
                    if ' - ' in v:
                        has_range_spaced = True
                    elif re.search(r'\d\s*-\s*[＄$€£¥]?\d', v):
                        has_range_nospace = True

                # --- Build SQL expression ---
                col_ref = f'"{col_name}"'
                expr = col_ref

                for prefix in sorted(prefixes_found, key=len, reverse=True):
                    expr = f"REPLACE({expr}, '{prefix}', '')"

                for sym in sorted(symbols_found):
                    expr = f"REPLACE({expr}, '{sym}', '')"

                expr = f"TRIM({expr})"
                cleaned_alias = expr

                if has_range_spaced or has_range_nospace:
                    sep = ' - ' if has_range_spaced else '-'

                    first_num = (
                        f"CASE WHEN INSTR({cleaned_alias}, '{sep}') > 0 "
                        f"THEN TRIM(SUBSTR({cleaned_alias}, 1, "
                        f"INSTR({cleaned_alias}, '{sep}') - 1)) "
                        f"ELSE {cleaned_alias} END"
                    )
                    second_num = (
                        f"CASE WHEN INSTR({cleaned_alias}, '{sep}') > 0 "
                        f"THEN TRIM(SUBSTR({cleaned_alias}, "
                        f"INSTR({cleaned_alias}, '{sep}') + {len(sep)})) "
                        f"ELSE {cleaned_alias} END"
                    )

                    for sym in sorted(symbols_found):
                        first_num = f"REPLACE({first_num}, '{sym}', '')"
                        second_num = f"REPLACE({second_num}, '{sym}', '')"

                    expr_min = f"CAST(REPLACE({first_num}, ',', '') AS REAL)"
                    expr_max = f"CAST(REPLACE({second_num}, ',', '') AS REAL)"
                else:
                    expr_min = f"CAST(REPLACE({cleaned_alias}, ',', '') AS REAL)"
                    expr_max = expr_min

                formats = []
                if has_range_spaced:
                    formats.append('range with " - " separator')
                if has_range_nospace:
                    formats.append('range with "-" separator (no spaces)')
                if prefixes_found:
                    formats.append(
                        f"prefixes: {', '.join(sorted(prefixes_found))}"
                    )
                if symbols_found:
                    formats.append(
                        f"currency symbols: {', '.join(sorted(symbols_found))}"
                    )
                if not formats:
                    formats.append("plain formatted numbers with commas")

                results.append({
                    "table": table,
                    "column": col_name,
                    "formats_found": formats,
                    "expr_min": expr_min,
                    "expr_max": expr_max,
                })

    finally:
        conn.close()

    return results


def get_cleaning_expressions(db_name: str = "data.db") -> str:
    """
    Return a human- and LLM-readable summary of SQL cleaning expressions
    for all numeric-as-text columns detected in the database.
    """
    entries = _detect_numeric_text_columns(db_name)
    if not entries:
        return "No numeric-as-text columns detected."

    parts = ["## SQL Cleaning Expressions\n"]
    for e in entries:
        parts.append(f"Column: {e['column']}  (table: {e['table']})")
        parts.append(f"  Formats found: {'; '.join(e['formats_found'])}")
        parts.append(f"  Expression for MINIMUM numeric value:")
        parts.append(f"    {e['expr_min']}")
        if e['expr_max'] != e['expr_min']:
            parts.append(f"  Expression for MAXIMUM numeric value:")
            parts.append(f"    {e['expr_max']}")
        parts.append("")

    parts.append("USE these expressions in ORDER BY, WHERE, MIN(), MAX(), etc.")
    parts.append("NEVER use plain ORDER BY on the original text column.")
    return "\n".join(parts)


def format_schema_for_llm(db_name: str = "data.db") -> str:
    """Format the full database schema as a readable string for the LLM.
    Automatically appends SQL cleaning expressions for numeric-as-text columns."""
    schema = get_schema_info(db_name)
    if not schema:
        return "No tables found in the database."

    parts = []
    for table in schema:
        lines = [f"Table: {table['table_name']} ({table['row_count']} rows)"]
        lines.append("-" * 50)
        for col in table["columns"]:
            nullable = "NULL" if col["nullable"] else "NOT NULL"
            pk = " [PK]" if col["primary_key"] else ""
            samples = ", ".join(col["sample_values"]) if col["sample_values"] else "N/A"
            lines.append(
                f"  {col['name']:30s} {col['type']:15s} {nullable:8s}{pk}"
            )
            lines.append(f"    Sample values: {samples}")
        parts.append("\n".join(lines))

    schema_text = "\n\n".join(parts)

    # Append programmatically-generated cleaning expressions
    cleaning = get_cleaning_expressions(db_name)
    if cleaning and "No numeric-as-text" not in cleaning:
        schema_text += "\n\n" + cleaning

    return schema_text
