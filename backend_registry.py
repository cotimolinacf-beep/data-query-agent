"""
Backend registry singleton.
Manages the active data backend (SQLite or BigQuery).
"""
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from backends.base import Backend
    from google.oauth2.credentials import Credentials


class BackendRegistry:
    """
    Singleton registry for the active data backend.

    Usage:
        # Set SQLite backend
        backend = BackendRegistry.set_sqlite_backend("data.db")
        backend.load_file("data.csv")

        # Set BigQuery backend
        backend = BackendRegistry.set_bigquery_backend(
            credentials=creds,
            project_id="my-project",
            dataset_id="my_dataset"
        )

        # Get current backend from anywhere
        backend = BackendRegistry.get_backend()
        result = backend.run_query("SELECT * FROM table")
    """

    _backend: Optional["Backend"] = None

    @classmethod
    def get_backend(cls) -> Optional["Backend"]:
        """Get the currently active backend."""
        return cls._backend

    @classmethod
    def set_sqlite_backend(cls, db_name: str = "data.db") -> "Backend":
        """Initialize and set SQLite as the active backend."""
        from backends.sqlite_backend import SQLiteBackend
        cls._backend = SQLiteBackend(db_name)
        return cls._backend

    @classmethod
    def set_bigquery_backend(
        cls,
        credentials: "Credentials",
        project_id: str,
        dataset_id: str,
    ) -> "Backend":
        """Initialize and set BigQuery as the active backend."""
        from backends.bigquery_backend import BigQueryBackend
        cls._backend = BigQueryBackend(
            credentials=credentials,
            project_id=project_id,
            dataset_id=dataset_id,
        )
        return cls._backend

    @classmethod
    def clear(cls) -> None:
        """Clear the active backend."""
        cls._backend = None

    @classmethod
    def is_configured(cls) -> bool:
        """Check if a backend is configured and connected."""
        return cls._backend is not None and cls._backend.is_connected()
