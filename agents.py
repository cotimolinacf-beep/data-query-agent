"""
LangGraph agent definitions: tools, nodes, and agent logic.
"""
from __future__ import annotations

import json
from typing import Annotated, Literal, Optional
from typing_extensions import TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph.message import add_messages

from db_manager import (
    load_file_to_sqlite,
    format_schema_for_llm,
    get_schema_info,
    run_query,
)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    file_path: str
    db_name: str
    table_info: str          # populated after ingestion
    schema_description: str  # populated after schema mapping
    current_agent: str       # tracks which agent is active
    custom_context: str      # user-provided document context (e.g. from DOCX)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@tool
def ingest_file(file_path: str, table_name: Optional[str] = None) -> str:
    """Load a CSV or Excel file into the SQLite database.
    Args:
        file_path: path to the .csv, .xls, or .xlsx file.
        table_name: optional name for the table (derived from filename if omitted).
    """
    result = load_file_to_sqlite(file_path, table_name=table_name)
    if result["success"]:
        return json.dumps({
            "status": "success",
            "table": result["table_name"],
            "columns": result["columns"],
            "rows_loaded": result["row_count"],
            "dtypes": result["dtypes"],
        })
    return json.dumps({"status": "error", "error": result["error"]})


@tool
def get_database_schema() -> str:
    """Return the full schema of the current SQLite database including sample values."""
    return format_schema_for_llm()


@tool
def execute_sql(query: str) -> str:
    """Execute a read-only SQL SELECT query against the database and return results.
    Args:
        query: a SQL SELECT statement. DML (INSERT/UPDATE/DELETE) is not allowed.
    """
    upper = query.strip().upper()
    if any(upper.startswith(kw) for kw in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE")):
        return json.dumps({"error": "Only SELECT queries are allowed."})

    result = run_query(query)
    if result["success"]:
        if "columns" in result:
            rows_preview = result["rows"][:50]  # cap preview
            return json.dumps({
                "columns": result["columns"],
                "rows": rows_preview,
                "total_rows": result["row_count"],
            })
        return json.dumps({"message": result.get("message", "OK")})
    return json.dumps({"error": result["error"]})


@tool
def list_tables() -> str:
    """List all tables in the database with their row counts."""
    schema = get_schema_info()
    tables = [{"name": t["table_name"], "rows": t["row_count"]} for t in schema]
    return json.dumps(tables)


# ---------------------------------------------------------------------------
# Tool lists per agent
# ---------------------------------------------------------------------------
INGESTION_TOOLS = [ingest_file]
SCHEMA_TOOLS = [get_database_schema, list_tables]
QUERY_TOOLS = [execute_sql, get_database_schema, list_tables]


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------
INGESTION_PROMPT = SystemMessage(content="""You are the Data Ingestion Agent.
Your job is to load the user's CSV or Excel file into a SQLite database.

Steps:
1. Use the ingest_file tool with the provided file path.
2. Report back the table name, columns, and number of rows loaded.

Always call the tool — do NOT try to read the file yourself.""")

SCHEMA_PROMPT = SystemMessage(content="""You are the Schema Mapping Agent.
Your job is to analyze the database schema and produce a clear, human-readable
description of each table and its columns.

Steps:
1. Call get_database_schema to retrieve full schema info with sample values.
   NOTE: The schema output already includes auto-generated "SQL Cleaning
   Expressions" for any TEXT columns that contain numeric data (prices, amounts,
   ranges, etc.).  These are created programmatically and are reliable.
2. For each column, describe:
   - What the column likely represents (infer from name + sample values).
   - The data type and whether it's nullable.
   - Any observations (e.g. "looks like a date", "categorical with few values").
3. If the schema includes a "SQL Cleaning Expressions" section, copy it verbatim
   into your output so it is passed along to the Query Agent.
4. Return a well-formatted summary.

Be thorough but concise.""")

QUERY_PROMPT = SystemMessage(content="""You are the SQL Query Agent.
Your ONLY job is to answer the user's questions by EXECUTING SQL queries
against the SQLite database using the execute_sql tool.

CRITICAL RULES:
- You MUST call the execute_sql tool to run queries. NEVER just write SQL as text.
- You MUST use the tool. Do NOT return SQL in your response without executing it.
- After getting results from execute_sql, present them clearly to the user.
- The sample values shown in the schema are only a SMALL SUBSET (3 rows).
  NEVER assume they represent all the data. ALWAYS query the database to find
  actual values. If the user asks about a specific item, search for it with SQL
  (e.g. WHERE column LIKE '%search_term%') instead of saying it doesn't exist.

The database schema and column descriptions are provided below.
Pay special attention to the "SQL Cleaning Expressions" section — it contains
ready-to-use SQLite expressions for columns that store numeric data as TEXT.

Steps for EVERY question:
1. Read the schema to understand tables and columns.
2. Build a correct SQLite SELECT query:
   - LIMIT to 20 rows unless the user asks for more.
   - Never SELECT *. Pick only needed columns.
3. CRITICAL — Numeric data stored as TEXT:
   NEVER use plain ORDER BY, MIN, MAX, or comparisons on text columns that
   contain numeric data (prices, amounts, etc.).  Text sorts alphabetically,
   so "9..." comes after "10..." and results will be WRONG.
   Instead, use the SQL cleaning expressions provided in the ADDITIONAL COLUMN
   DESCRIPTIONS section.  These expressions were built by inspecting the actual
   data in the file and handle every format variation present.
   If no cleaning expression is provided for a column you suspect is numeric,
   first run SELECT DISTINCT <column> FROM <table> LIMIT 30 to inspect the
   formats, then build your own REPLACE/CAST expression before sorting.
4. CALL execute_sql with the query. This is mandatory.
5. If the query fails, fix it and call execute_sql again (up to 3 retries).
6. Present the actual results from the tool response. Do NOT invent data.

NEVER run DML statements (INSERT, UPDATE, DELETE, DROP).""")


# ---------------------------------------------------------------------------
# Domain context: Conversation Audit Data (Auditoría de conversaciones)
# ---------------------------------------------------------------------------
CONVERSATION_AUDIT_CONTEXT = """
=== CONTEXTO DE DOMINIO: AUDITORÍA DE CONVERSACIONES ===

Esta tabla contiene información de conversaciones entre clientes y la empresa,
a través de distintos canales de comunicación (WhatsApp, chat web, etc.).
Cada registro representa una conversación individual. Un mismo cliente puede
tener múltiples conversaciones.

La tabla permite analizar:
- Volumen y origen de conversaciones
- Comportamiento de clientes
- Desempeño de agentes y grupos
- Uso de bots y flujos automatizados
- Tipificaciones, etiquetas y resultados comerciales (ventas)

== DICCIONARIO DE CAMPOS (nombre_columna → descripción) ==

--- Identificación de conversación y cliente ---
conversation_id        : URL/ID único de la conversación en la plataforma
cliente                : Nombre del cliente (puede ser emoji o nombre real)
clientname             : Nombre del cliente (campo alternativo)
telefono               : Número de teléfono del cliente
email                  : Email del cliente
identificacion         : Documento de identificación del cliente

--- Canal y empresa ---
canal                  : Teléfono/identificador del canal de comunicación
bot                    : Nombre del bot que atendió inicialmente

--- Atención humana (agentes y grupos) ---
grupo                  : Nombre del grupo que atendió la conversación
                         (ej: Ventas, venta_motos, Servicio Técnico, Compras en Línea,
                          Taller de Motos, Carbone Motors ventas, Carbone motors taller o repuestos)
agente                 : Nombre del agente que atendió la conversación

--- Fechas (formato ISO: YYYY-MM-DD HH:MM:SS) ---
fecha_inicio_gestión   : Fecha y hora del primer mensaje de la conversación
fecha_asignación       : Fecha y hora en que la conversación fue asignada a un agente
  NOTA: Las fechas se almacenan en formato ISO (2025-12-22 16:13:00).
  Para filtrar por fecha usa: WHERE fecha_inicio_gestión >= '2025-12-01'
  Para extraer partes: strftime('%Y-%m', fecha_inicio_gestión) para año-mes,
                       strftime('%w', fecha_inicio_gestión) para día de la semana (0=domingo),
                       strftime('%H', fecha_inicio_gestión) para hora del día.

--- Estado y resultado de la conversación ---
estado_gestión         : Estado actual de la gestión.
                         Valores posibles: CERRADO, NUEVO, REINGRESO, REASIGNADO, ABIERTO
venta                  : Indica si la conversación terminó en venta (Sí / No)

--- Bot y automatización ---
finalización_flujo     : Cómo terminó el flujo del bot.
                         Valores posibles: Asignacion (pasó a agente humano),
                         Autogestion (se resolvió solo), Abandono (cliente abandonó)
última_tipificación_flujo          : Última tipificación asignada por el flujo del bot
última_tipificación_keyword_flujo  : Keyword de la última tipificación del flujo del bot

--- Tipificaciones y clasificación ---
grupo_tipificación     : Grupo de tipificación
primer_respuesta       : Primera tipificación/respuesta registrada
segunda_respuesta      : Segunda tipificación/respuesta registrada
tercer_respuesta       : Tercera tipificación/respuesta registrada
última_tipificación    : Última tipificación asignada a la conversación
                         (ej: Consulta General, Positivo Encuesta, No tipificado,
                          Cliente no respondió, cotización enviada, No Interesado,
                          Gestión resuelta, Cierre de conversación automático)
última_tipificación_keyword : Keyword corto de la última tipificación
                         (ej: CG, Encuesta, NO_TIPIFICADO, csr, COT, NI, nc, gr)
ult_mensaje_previo_abandono : Último mensaje enviado antes de que el cliente abandonara

--- Etiquetas ---
última_etiqueta        : Última etiqueta asignada a la conversación

--- Etapas del journey del cliente ---
maxima_etapa           : Máxima etapa alcanzada por el cliente en el journey comercial.
                         Valores posibles: SQL (Sales Qualified Lead),
                         Lead (prospecto), MQL (Marketing Qualified Lead)

--- Producto y comercial ---
producto               : Producto o servicio de interés del cliente
modelo                 : Modelo específico del producto
modelodemoto           : Modelo de moto (campo específico)
cotizacin              : Información de cotización
placa                  : Placa del vehículo
chasis_moto            : Número de chasis de la moto
orden_servicio         : Número de orden de servicio
saldopendiente         : Saldo pendiente
saldopendientec4n      : Saldo pendiente (formato C4N)

--- Encuestas y satisfacción ---
comentario_encuesta    : Comentarios de la encuesta de satisfacción
excelente              : Calificación "Excelente" en encuesta
regular                : Calificación "Regular" en encuesta
calificaciongeneral    : Calificación general de la encuesta
satisfaccion           : Nivel de satisfacción

--- Campos custom / RRHH ---
customantiguedad              : Antigüedad del colaborador
customcategoriadeinteres      : Categoría de interés
customcargoactual             : Cargo actual del colaborador
customdireccion               : Dirección
customexperimentotrafico      : Experimento de tráfico / tipo de origen
customvacantedeinteres        : Vacante de interés
departamento                  : Departamento
numerodecolaborador           : Número de colaborador
sucursaldecartadetrabajo      : Sucursal de carta de trabajo
destinocartadetrabajo         : Destino de carta de trabajo
bienvenida                    : Campo de bienvenida

== KPIs Y MÉTRICAS CALCULABLES ==

Al responder preguntas, ten en cuenta estos KPIs comunes del dominio:

1. VOLUMEN Y ACTIVIDAD
   - Total de conversaciones: COUNT(*)
   - Conversaciones por canal: GROUP BY canal
   - Conversaciones por período: GROUP BY strftime('%Y-%m', fecha_inicio_gestión)
   - Conversaciones por día de la semana: GROUP BY strftime('%w', fecha_inicio_gestión)
   - Conversaciones por hora del día: GROUP BY strftime('%H', fecha_inicio_gestión)

2. CLIENTES
   - Clientes únicos: COUNT(DISTINCT telefono)
   - Conversaciones promedio por cliente: COUNT(*) / COUNT(DISTINCT telefono)

3. DESEMPEÑO DE AGENTES Y GRUPOS
   - Conversaciones atendidas por agente: GROUP BY agente
   - Conversaciones atendidas por grupo: GROUP BY grupo
   - Ranking de agentes por volumen

4. VENTAS Y RESULTADOS
   - Tasa de conversión a venta: SUM(CASE WHEN venta='Sí' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
   - Ventas por agente, grupo, producto, etc.

5. TIPIFICACIÓN Y CALIDAD
   - % de conversaciones tipificadas vs no tipificadas
   - Tipificaciones más frecuentes: GROUP BY última_tipificación
   - Distribución de etiquetas

6. BOT Y AUTOMATIZACIÓN
   - Distribución de finalización del flujo: GROUP BY finalización_flujo
   - Tasa de autogestión: conversaciones con finalización_flujo='Autogestion'
   - Tasa de abandono: conversaciones con finalización_flujo='Abandono'

7. JOURNEY DEL CLIENTE
   - Distribución por etapa máxima: GROUP BY maxima_etapa
   - Conversiones por etapa (Lead → MQL → SQL)

8. PRODUCTOS
   - Productos más consultados: GROUP BY producto ORDER BY COUNT(*) DESC

== INSTRUCCIONES ESPECIALES ==
- Responde SIEMPRE en español.
- Cuando el usuario pregunte por "origen" o "tipo de origen", usa la columna customexperimentotrafico.
- Cuando pregunte por "etapa" o "stage", usa maxima_etapa.
- Cuando pregunte por "tipificación", usa última_tipificación o última_tipificación_keyword.
- Los nombres de agentes pueden tener espacios extra; usa TRIM() o LIKE para buscar.
- Muchos campos custom están vacíos para la mayoría de registros; indica esto al usuario si es relevante.
"""
