"""
Streamlit UI for the Data Query Agent system.
Usage:  streamlit run app.py
"""
import os
import tempfile

import streamlit as st

# ---------------------------------------------------------------------------
# Load secrets into env vars (for Streamlit Cloud compatibility)
# Locally, .env is used via python-dotenv. On Streamlit Cloud, secrets are
# defined in the dashboard and accessed via st.secrets.
# ---------------------------------------------------------------------------
try:
    for key in ("GOOGLE_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
        if key in st.secrets and key not in os.environ:
            os.environ[key] = st.secrets[key]
except FileNotFoundError:
    pass  # No secrets file — .env will be used instead

from graph import DataQuerySystem
from db_manager import get_column_summary
from backend_registry import BackendRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_docx_text(file_bytes: bytes) -> str:
    """Extract plain text from a .docx file."""
    from docx import Document
    import io
    doc = Document(io.BytesIO(file_bytes))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


def build_context_from_descriptions(columns: list[dict], descriptions: dict) -> str:
    """Build a context string from user-provided column descriptions."""
    lines = [
        "== DICCIONARIO DE CAMPOS (proporcionado por el usuario) ==\n"
    ]
    for col in columns:
        name = col["column"]
        desc = descriptions.get(name, "").strip()
        if desc:
            lines.append(f"{name} : {desc}")
        else:
            lines.append(
                f"{name} : (sin descripción — tipo {col['type']}, "
                f"{col['fill_pct']}% con datos, ej: {', '.join(col['samples'][:3])})"
            )
    return "\n".join(lines)


def get_bigquery_credentials():
    """
    Get BigQuery credentials from Streamlit secrets.
    Supports service account JSON for Streamlit Cloud.
    """
    try:
        from google.oauth2 import service_account

        # Check if service account credentials are in secrets
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
            )
            return credentials
    except Exception as e:
        st.error(f"Error loading credentials: {e}")
    return None


def reset_to_source_selection():
    """Reset state to go back to source selection."""
    BackendRegistry.clear()
    for key in ["data_source", "system", "messages", "schema_description",
                "csv_loaded", "context_ready", "column_info", "col_descriptions",
                "doc_context", "bq_connected", "bq_project", "bq_dataset"]:
        if key in st.session_state:
            del st.session_state[key]


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Data Query Agent",
    page_icon="🔍",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
defaults = {
    "data_source": None,  # None, "csv", or "bigquery"
    "system": None,
    "messages": [],
    "schema_description": "",
    "doc_context": "",
    "csv_loaded": False,
    "context_ready": False,
    "column_info": [],
    "col_descriptions": {},
    # BigQuery specific
    "bq_connected": False,
    "bq_project": "",
    "bq_dataset": "",
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val


# ---------------------------------------------------------------------------
# Source Selection Screen
# ---------------------------------------------------------------------------
if st.session_state.data_source is None:
    st.title("Data Query Agent")
    st.write("Selecciona la fuente de datos para comenzar:")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📁 Archivo CSV/Excel")
        st.write("Carga un archivo local para analizarlo con SQL.")
        st.write("")
        if st.button("Usar CSV/Excel", key="btn_csv", use_container_width=True, type="primary"):
            st.session_state.data_source = "csv"
            BackendRegistry.set_sqlite_backend()
            st.rerun()

    with col2:
        st.markdown("### ☁️ Google BigQuery")
        st.write("Conectate a tus datos en BigQuery (solo lectura).")
        st.write("")
        if st.button("Usar BigQuery", key="btn_bq", use_container_width=True, type="primary"):
            st.session_state.data_source = "bigquery"
            st.rerun()

    st.stop()


# ---------------------------------------------------------------------------
# Sidebar (depends on data source)
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Configuración")

    # Back button to change data source
    if st.button("← Cambiar fuente de datos", use_container_width=True):
        reset_to_source_selection()
        st.rerun()

    st.divider()

    # =========================================================================
    # CSV/Excel Flow
    # =========================================================================
    if st.session_state.data_source == "csv":
        # --- 1. Document context (optional) ---
        st.subheader("1. Documento de contexto")
        st.caption("Sube un .docx que describa los campos de tu tabla (opcional)")
        doc_file = st.file_uploader(
            "Documento de contexto",
            type=["docx"],
            key="doc_uploader",
            label_visibility="collapsed",
        )

        if doc_file is not None and not st.session_state.doc_context:
            with st.spinner("Leyendo documento..."):
                text = extract_docx_text(doc_file.read())
                st.session_state.doc_context = text

        if st.session_state.doc_context:
            st.success(f"Contexto cargado ({len(st.session_state.doc_context):,} chars)")
            with st.expander("Ver contexto", expanded=False):
                st.text(st.session_state.doc_context[:2000] + (
                    "\n..." if len(st.session_state.doc_context) > 2000 else ""
                ))

        st.divider()

        # --- 2. CSV upload ---
        st.subheader("2. Archivo de datos")
        st.caption("Sube un CSV o Excel con tus datos")
        csv_file = st.file_uploader(
            "Archivo CSV o Excel",
            type=["csv", "xls", "xlsx"],
            key="csv_uploader",
            label_visibility="collapsed",
        )

        load_btn = st.button(
            "Cargar en base de datos",
            type="primary",
            disabled=csv_file is None,
            use_container_width=True,
        )

        if load_btn and csv_file is not None:
            suffix = os.path.splitext(csv_file.name)[1]
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(csv_file.read())
                tmp_path = tmp.name

            with st.spinner("Cargando datos y analizando esquema..."):
                # Ensure SQLite backend is set
                backend = BackendRegistry.get_backend()
                if backend is None:
                    backend = BackendRegistry.set_sqlite_backend()

                # Load file into backend
                result = backend.load_file(tmp_path)

                if result.get("success"):
                    system = DataQuerySystem(backend_type="sqlite")
                    # If DOCX context available, set it now
                    if st.session_state.doc_context:
                        system.set_context(st.session_state.doc_context)
                    schema = system.ingest(tmp_path)

                    st.session_state.system = system
                    st.session_state.schema_description = schema
                    st.session_state.csv_loaded = True
                    st.session_state.messages = []

                    # If DOCX was provided, context is ready.
                    if st.session_state.doc_context:
                        st.session_state.context_ready = True
                    else:
                        st.session_state.context_ready = False
                        st.session_state.column_info = backend.get_column_summary(
                            table_name=result.get("table_name")
                        )
                        st.session_state.col_descriptions = {}

                    st.success(f"**{csv_file.name}** cargado")
                else:
                    st.error(f"Error: {result.get('error')}")

            os.unlink(tmp_path)
            st.rerun()

        if st.session_state.schema_description:
            with st.expander("Ver esquema de la DB", expanded=False):
                st.text(st.session_state.schema_description[:3000] + (
                    "\n..." if len(st.session_state.schema_description) > 3000 else ""
                ))

    # =========================================================================
    # BigQuery Flow
    # =========================================================================
    elif st.session_state.data_source == "bigquery":
        st.subheader("☁️ Google BigQuery")

        # Check for credentials
        credentials = get_bigquery_credentials()

        if credentials is None:
            st.warning("No se encontraron credenciales de BigQuery.")
            st.markdown("""
            **Para usar BigQuery:**

            1. Crea un Service Account en GCP con permisos de BigQuery Reader
            2. Descarga el JSON de credenciales
            3. En Streamlit Cloud: Settings → Secrets
            4. Agrega las credenciales con el formato:

            ```toml
            [gcp_service_account]
            type = "service_account"
            project_id = "tu-proyecto"
            private_key_id = "..."
            private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
            client_email = "..."
            client_id = "..."
            auth_uri = "https://accounts.google.com/o/oauth2/auth"
            token_uri = "https://oauth2.googleapis.com/token"
            ```
            """)
            st.stop()

        st.success("Credenciales de GCP encontradas")

        # Project and Dataset inputs
        project_id = st.text_input(
            "Project ID",
            value=st.session_state.bq_project,
            placeholder="mi-proyecto-gcp"
        )

        dataset_id = st.text_input(
            "Dataset ID",
            value=st.session_state.bq_dataset,
            placeholder="mi_dataset"
        )

        connect_btn = st.button(
            "Conectar a BigQuery",
            type="primary",
            disabled=not project_id or not dataset_id,
            use_container_width=True,
        )

        if connect_btn and project_id and dataset_id:
            with st.spinner("Conectando a BigQuery..."):
                try:
                    backend = BackendRegistry.set_bigquery_backend(
                        credentials=credentials,
                        project_id=project_id,
                        dataset_id=dataset_id,
                    )

                    if backend.is_connected():
                        st.session_state.bq_project = project_id
                        st.session_state.bq_dataset = dataset_id
                        st.session_state.bq_connected = True

                        # Initialize system
                        system = DataQuerySystem(backend_type="bigquery")
                        schema = system.connect_bigquery()

                        st.session_state.system = system
                        st.session_state.schema_description = schema
                        st.session_state.csv_loaded = True  # Reuse this flag
                        st.session_state.context_ready = True
                        st.session_state.messages = []

                        st.success("Conectado a BigQuery!")
                        st.rerun()
                    else:
                        st.error("No se pudo conectar. Verifica proyecto y dataset.")
                except Exception as e:
                    st.error(f"Error: {e}")

        # Show tables if connected
        if st.session_state.bq_connected:
            backend = BackendRegistry.get_backend()
            if backend and backend.is_connected():
                st.divider()
                st.subheader("Tablas disponibles")
                tables = backend.get_tables_list()
                for table in tables:
                    st.write(f"• **{table['name']}** ({table['rows']:,} filas)")

                with st.expander("Ver esquema completo", expanded=False):
                    st.text(st.session_state.schema_description[:3000] + (
                        "\n..." if len(st.session_state.schema_description) > 3000 else ""
                    ))

    st.divider()

    if st.session_state.csv_loaded and st.button("Limpiar conversación", use_container_width=True):
        st.session_state.messages = []
        if st.session_state.system:
            st.session_state.system.messages_history = []
        st.rerun()


# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------
source_label = "BigQuery" if st.session_state.data_source == "bigquery" else "CSV/Excel"
st.title(f"Data Query Agent ({source_label})")

# --- State: no data loaded ---
if not st.session_state.csv_loaded:
    if st.session_state.data_source == "csv":
        st.info(
            "**Para comenzar:**\n"
            "1. (Opcional) Sube un documento .docx con la descripción de los campos\n"
            "2. Sube un archivo CSV o Excel con tus datos\n"
            "3. Haz clic en *Cargar en base de datos*"
        )
    else:
        st.info(
            "**Para comenzar:**\n"
            "1. Ingresa tu Project ID y Dataset ID de BigQuery\n"
            "2. Haz clic en *Conectar a BigQuery*"
        )

# --- State: CSV loaded but no context (no DOCX) — show column description form ---
elif st.session_state.data_source == "csv" and not st.session_state.context_ready:
    cols = st.session_state.column_info

    st.warning(
        f"No se cargó un documento de contexto. "
        f"Describe los campos que necesites para que el agente entienda tus datos. "
        f"Los campos vacíos se dejarán sin descripción."
    )
    st.caption(
        f"Se encontraron **{len(cols)}** columnas con datos. "
        f"Solo se muestran las que tienen al menos un valor."
    )

    with st.form("col_descriptions_form"):
        descriptions = {}
        for col in cols:
            name = col["column"]
            samples_str = ", ".join(col["samples"][:4])
            help_text = (
                f"Tipo: {col['type']} — "
                f"{col['fill_pct']}% con datos — "
                f"Ejemplos: {samples_str}"
            )
            descriptions[name] = st.text_input(
                name,
                value=st.session_state.col_descriptions.get(name, ""),
                help=help_text,
                placeholder="Describe qué representa este campo...",
            )

        submitted = st.form_submit_button(
            "Confirmar descripciones y comenzar a chatear",
            type="primary",
            use_container_width=True,
        )

        if submitted:
            st.session_state.col_descriptions = descriptions
            context = build_context_from_descriptions(cols, descriptions)
            st.session_state.system.set_context(context)
            st.session_state.context_ready = True
            st.rerun()

# --- State: ready to chat ---
elif st.session_state.context_ready:
    st.caption("Haz preguntas sobre tus datos en lenguaje natural")

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Escribe tu pregunta..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Consultando..."):
                answer = st.session_state.system.ask(prompt)
            st.markdown(answer)

        st.session_state.messages.append({"role": "assistant", "content": answer})
