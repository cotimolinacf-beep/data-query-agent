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
    "system": None,
    "messages": [],
    "schema_description": "",
    "doc_context": "",
    "csv_loaded": False,
    "context_ready": False,
    "column_info": [],
    "col_descriptions": {},
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Configuración")

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
            system = DataQuerySystem()
            # If DOCX context available, set it now
            if st.session_state.doc_context:
                system.set_context(st.session_state.doc_context)
            schema = system.ingest(tmp_path)

        os.unlink(tmp_path)

        st.session_state.system = system
        st.session_state.schema_description = schema
        st.session_state.csv_loaded = True
        st.session_state.messages = []

        # If DOCX was provided, context is ready. Otherwise, need manual descriptions.
        if st.session_state.doc_context:
            st.session_state.context_ready = True
        else:
            st.session_state.context_ready = False
            st.session_state.column_info = get_column_summary(
                table_name=system.table_name
            )
            st.session_state.col_descriptions = {}

        st.success(f"**{csv_file.name}** cargado")
        st.rerun()

    if st.session_state.schema_description:
        with st.expander("Ver esquema de la DB", expanded=False):
            st.text(st.session_state.schema_description[:3000] + (
                "\n..." if len(st.session_state.schema_description) > 3000 else ""
            ))

    st.divider()

    if st.button("Limpiar conversación", use_container_width=True):
        st.session_state.messages = []
        if st.session_state.system:
            st.session_state.system.messages_history = []
        st.rerun()


# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------
st.title("Data Query Agent")

# --- State: no CSV loaded ---
if not st.session_state.csv_loaded:
    st.info(
        "**Para comenzar:**\n"
        "1. (Opcional) Sube un documento .docx con la descripción de los campos\n"
        "2. Sube un archivo CSV o Excel con tus datos\n"
        "3. Haz clic en *Cargar en base de datos*"
    )

# --- State: CSV loaded but no context (no DOCX) — show column description form ---
elif not st.session_state.context_ready:
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
else:
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
