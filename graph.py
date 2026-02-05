"""
LangGraph multi-agent graph: wires Ingestion -> Schema Mapping -> Query loop.
"""
import json
import os
from dotenv import load_dotenv

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode

from agents import (
    AgentState,
    INGESTION_PROMPT,
    INGESTION_TOOLS,
    SCHEMA_PROMPT,
    SCHEMA_TOOLS,
    QUERY_PROMPT,
    QUERY_TOOLS,
    CONVERSATION_AUDIT_CONTEXT,
)

load_dotenv()


# ---------------------------------------------------------------------------
# LLM factory
# ---------------------------------------------------------------------------
def _get_llm():
    """Return an LLM based on available API keys."""
    if os.getenv("GOOGLE_API_KEY"):
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model="gpt-4o-mini", temperature=0)

    if os.getenv("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model="claude-sonnet-4-20250514", temperature=0)

    raise EnvironmentError(
        "No LLM API key found. Set GOOGLE_API_KEY, OPENAI_API_KEY, or ANTHROPIC_API_KEY in .env"
    )


# ---------------------------------------------------------------------------
# Agent node helpers
# ---------------------------------------------------------------------------
def _call_agent(state: AgentState, system_msg, tools) -> dict:
    """Generic: invoke the LLM with system prompt + state messages + tools."""
    llm = _get_llm().bind_tools(tools)
    messages = [system_msg] + state["messages"]
    response = llm.invoke(messages)
    return {"messages": [response]}


# ---------------------------------------------------------------------------
# Node: Ingestion Agent
# ---------------------------------------------------------------------------
def ingestion_agent(state: AgentState) -> dict:
    """Asks the LLM to call the ingest_file tool."""
    file_path = state.get("file_path", "")
    # Inject a user message so the LLM knows the file path
    inject = HumanMessage(content=f"Please load this file into the database: {file_path}")
    new_state = {**state, "messages": state["messages"] + [inject]}
    return _call_agent(new_state, INGESTION_PROMPT, INGESTION_TOOLS)


def ingestion_router(state: AgentState) -> str:
    """After ingestion agent responds, check if it wants to call a tool."""
    last = state["messages"][-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "ingestion_tools"
    return "schema_agent"


# ---------------------------------------------------------------------------
# Node: Schema Mapping Agent
# ---------------------------------------------------------------------------
def schema_agent(state: AgentState) -> dict:
    return _call_agent(state, SCHEMA_PROMPT, SCHEMA_TOOLS)


def schema_router(state: AgentState) -> str:
    last = state["messages"][-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "schema_tools"
    # Schema description is now in the last AI message
    return "wait_for_question"


# ---------------------------------------------------------------------------
# Node: Wait for user question (passthrough — ends the ingestion pipeline)
# ---------------------------------------------------------------------------
def wait_for_question(state: AgentState) -> dict:
    """Marker node: the ingestion + schema pipeline is done."""
    # Capture the schema description from the last AI message for later use
    for msg in reversed(state["messages"]):
        if isinstance(msg, AIMessage) and not getattr(msg, "tool_calls", None):
            return {"schema_description": msg.content}
    return {}


# ---------------------------------------------------------------------------
# Node: Query Agent
# ---------------------------------------------------------------------------
def _detect_conversation_audit(raw_schema: str) -> bool:
    """Return True if the loaded data looks like conversation audit data."""
    indicators = [
        "conversation_id", "estado_gestión", "última_tipificación",
        "finalización_flujo", "agente", "grupo", "venta",
    ]
    lower = raw_schema.lower()
    hits = sum(1 for ind in indicators if ind in lower)
    return hits >= 4


def query_agent(state: AgentState) -> dict:
    from langchain_core.messages import SystemMessage
    from backend_registry import BackendRegistry

    # Get schema from the active backend (SQLite or BigQuery)
    backend = BackendRegistry.get_backend()
    if backend:
        raw_schema = backend.format_schema_for_llm()
    else:
        raw_schema = "No data source configured."

    schema_desc = state.get("schema_description", "")
    custom_ctx = state.get("custom_context", "")
    backend_type = state.get("backend_type", "sqlite")

    # Adjust prompt for BigQuery vs SQLite
    if backend_type == "bigquery":
        schema_context = QUERY_PROMPT.content.replace(
            "SQLite database",
            "BigQuery database"
        ).replace(
            "SQLite SELECT",
            "BigQuery SELECT"
        )
    else:
        schema_context = QUERY_PROMPT.content

    # Inject user-provided context (from DOCX or manual column descriptions)
    if custom_ctx:
        schema_context += (
            f"\n\n=== CONTEXTO DE DOMINIO (proporcionado por el usuario) ===\n"
            f"{custom_ctx}\n"
            f"=== FIN DEL CONTEXTO ===\n"
            f"\nUsa esta información para entender el significado de cada campo, "
            f"los KPIs relevantes, y cómo responder las preguntas del usuario. "
            f"Responde SIEMPRE en español."
        )

    schema_context += f"\n\nDATABASE SCHEMA (tables, columns, types, sample values):\n{raw_schema}"
    if schema_desc:
        schema_context += f"\n\nADDITIONAL COLUMN DESCRIPTIONS:\n{schema_desc}"

    system = SystemMessage(content=schema_context)
    return _call_agent(state, system, QUERY_TOOLS)


def query_router(state: AgentState) -> str:
    last = state["messages"][-1]
    if hasattr(last, "tool_calls") and last.tool_calls:
        return "query_tools"
    return END


# ---------------------------------------------------------------------------
# Build the INGESTION graph (run once when a file is uploaded)
# ---------------------------------------------------------------------------
def build_ingestion_graph():
    """Graph: file upload -> ingest -> schema mapping -> done."""
    ingestion_tool_node = ToolNode(INGESTION_TOOLS)
    schema_tool_node = ToolNode(SCHEMA_TOOLS)

    g = StateGraph(AgentState)

    g.add_node("ingestion_agent", ingestion_agent)
    g.add_node("ingestion_tools", ingestion_tool_node)
    g.add_node("schema_agent", schema_agent)
    g.add_node("schema_tools", schema_tool_node)
    g.add_node("wait_for_question", wait_for_question)

    g.add_edge(START, "ingestion_agent")
    g.add_conditional_edges("ingestion_agent", ingestion_router)
    g.add_edge("ingestion_tools", "schema_agent")
    g.add_conditional_edges("schema_agent", schema_router)
    g.add_edge("schema_tools", "schema_agent")  # loop back after tool call
    g.add_edge("wait_for_question", END)

    return g.compile()


# ---------------------------------------------------------------------------
# Build the QUERY graph (run for each user question)
# ---------------------------------------------------------------------------
def build_query_graph():
    """Graph: user question -> SQL agent -> tool -> answer (with retry loop)."""
    query_tool_node = ToolNode(QUERY_TOOLS)

    g = StateGraph(AgentState)

    g.add_node("query_agent", query_agent)
    g.add_node("query_tools", query_tool_node)

    g.add_edge(START, "query_agent")
    g.add_conditional_edges("query_agent", query_router)
    g.add_edge("query_tools", "query_agent")  # loop back for multi-step

    return g.compile()


# ---------------------------------------------------------------------------
# High-level API
# ---------------------------------------------------------------------------
class DataQuerySystem:
    """Orchestrates the full pipeline: ingest file, map schema, answer questions."""

    def __init__(self, backend_type: str = "sqlite"):
        self.backend_type = backend_type
        self.ingestion_graph = build_ingestion_graph()
        self.query_graph = build_query_graph()
        self.schema_description = ""
        self.custom_context = ""
        self.table_name = ""
        self.messages_history: list = []

    def set_context(self, context: str):
        """Set custom document context (e.g. extracted from a DOCX)."""
        self.custom_context = context

    def ingest(self, file_path: str) -> str:
        """Run the ingestion + schema mapping pipeline. Returns schema summary."""
        import json as _json

        state: AgentState = {
            "messages": [],
            "file_path": file_path,
            "db_name": "data.db",
            "table_info": "",
            "schema_description": "",
            "current_agent": "ingestion",
            "custom_context": self.custom_context,
            "backend_type": self.backend_type,
        }

        result = self.ingestion_graph.invoke(state)
        self.schema_description = result.get("schema_description", "")
        self.messages_history = result.get("messages", [])

        # Extract table name from tool messages
        for msg in result.get("messages", []):
            if isinstance(msg, ToolMessage):
                try:
                    data = _json.loads(msg.content)
                    if "table" in data:
                        self.table_name = data["table"]
                        break
                except (ValueError, TypeError):
                    pass

        return self.schema_description

    def connect_bigquery(self) -> str:
        """
        For BigQuery: skip ingestion, just get schema from the connected backend.
        Returns the schema description.
        """
        from backend_registry import BackendRegistry

        self.backend_type = "bigquery"
        backend = BackendRegistry.get_backend()

        if backend and backend.is_connected():
            self.schema_description = backend.format_schema_for_llm()
            # Get first table name if any
            tables = backend.get_tables_list()
            if tables:
                self.table_name = tables[0]["name"]
        else:
            self.schema_description = "BigQuery not connected."

        return self.schema_description

    def ask(self, question: str, verbose: bool = False) -> str:
        """Ask a natural-language question about the loaded data."""
        self.messages_history.append(HumanMessage(content=question))

        state: AgentState = {
            "messages": list(self.messages_history),
            "file_path": "",
            "db_name": "data.db",
            "table_info": "",
            "schema_description": self.schema_description,
            "current_agent": "query",
            "custom_context": self.custom_context,
            "backend_type": self.backend_type,
        }

        result = self.query_graph.invoke(state)

        # Extract the final AI answer
        answer = "No answer could be generated."
        for msg in reversed(result.get("messages", [])):
            if isinstance(msg, AIMessage) and not getattr(msg, "tool_calls", None):
                answer = msg.content
                break

        self.messages_history.append(AIMessage(content=answer))

        return answer
