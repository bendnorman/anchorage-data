import os
import logging
from pathlib import Path
from dagster import IOManager, io_manager, Field, MemoizableIOManager
import sqlalchemy as sa
import pandas as pd

from sqlite3 import Connection as SQLite3Connection
from sqlite3 import sqlite_version
from packaging import version

logger = logging.getLogger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"


class SQLiteIOManager(MemoizableIOManager):
    """Built-in filesystem IO manager that stores and retrieves values using pickling.

    Args:
        base_dir (Optional[str]): base directory where all the step outputs which use this object
            manager will be stored in.
    """

    def __init__(
        self,
        base_dir: str = None,
        db_name: str = None,
        check_foreign_keys: bool = True,
        check_types: bool = True,
        check_values: bool = True,
    ):
        self.base_dir = Path(base_dir)
        self.db_name = db_name
        self.check_foreign_keys = check_foreign_keys

        bad_sqlite_version = version.parse(sqlite_version) < version.parse(
            MINIMUM_SQLITE_VERSION
        )
        if bad_sqlite_version and check_types:
            check_types = False
            logger.warning(
                f"Found SQLite {sqlite_version} which is less than "
                f"the minimum required version {MINIMUM_SQLITE_VERSION} "
                "As a result, data type constraint checking has been disabled."
            )

    def _setup_database(self, engine):
        @sa.event.listens_for(sa.engine.Engine, "connect")
        def _set_sqlite_pragma(dbapi_connection, connection_record):
            if isinstance(dbapi_connection, SQLite3Connection):
                cursor = dbapi_connection.cursor()
                cursor.execute(
                    f"PRAGMA foreign_keys={'ON' if self.check_foreign_keys else 'OFF'};"
                )
                cursor.close()

    def _get_database_engine(self, context) -> sa.engine.Engine:
        """Create database and metadata if they don't exist."""
        # TODO: is the run_id gauranteed to be the first one?
        run_id = context.get_identifier()[0]
        run_dir = self.base_dir / run_id / "sqlite"

        # If the sqlite directory doesn't exist, create it.
        if not run_dir.exists():
            run_dir.mkdir(parents=True)
        db_path = run_dir / f"{self.db_name}.sqlite"

        engine = sa.create_engine(f"sqlite:///{db_path}")

        # If the database doesn't exist, create it
        if not db_path.exists():
            db_path.touch()

            self._setup_database(engine)

        return engine

    def has_output(self, context):
        table_name = context.get_asset_identifier()[0]
        engine = self._get_database_engine(context)
        inspector = sa.inspect(engine)
        return inspector.has_table(table_name)

    def handle_output(self, context, obj):
        table_name = context.get_asset_identifier()[0]

        engine = self._get_database_engine(context)
        with engine.connect() as con:
            obj.to_sql(
                table_name, con, if_exists="replace", index=False, chunksize=10_000
            )

    def load_input(self, context):
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = context.get_asset_identifier()[0]

        engine = self._get_database_engine(context)
        with engine.connect() as con:
            return pd.read_sql_table(table_name, con)


@io_manager(
    config_schema={
        "output_path": Field(
            str,
            default_value=str(Path(__file__).parents[2] / "data" / "outputs"),
        ),
        "check_foreign_keys": Field(bool, default_value=True),
        "check_types": Field(bool, default_value=True),
        "check_values": Field(bool, default_value=True),
    }
)
def ancdata_sqlite_io_manager(init_context) -> SQLiteIOManager:
    """Creatse a SQLiteManager dagster resource."""
    base_dir = init_context.resource_config["output_path"]
    check_foreign_keys = init_context.resource_config["check_foreign_keys"]
    check_types = init_context.resource_config["check_types"]
    check_values = init_context.resource_config["check_values"]
    return SQLiteIOManager(
        base_dir=base_dir,
        db_name="ancdata",
        check_foreign_keys=check_foreign_keys,
        check_types=check_types,
        check_values=check_values,
    )
