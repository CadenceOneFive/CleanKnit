import pysqlite3
from sqlalchemy import create_engine
from structlog import get_logger

log = get_logger()


# Copy the named schema from our application to a file-based database
# xref https://stackoverflow.com/a/67162137/40387

def backup_schema(S, schema_name, url):
    engine_backup_file = create_engine(
        url, module=pysqlite3
    )
    raw_connection_backup_file = engine_backup_file.raw_connection()
    raw_connection_socrata_resource = S.bind.raw_connection()

    log.info(f"Backing up {schema_name} to {url}")
    raw_connection_socrata_resource.backup(
        raw_connection_backup_file.connection, name=schema_name
    )
    log.info(f"done with {schema_name}")

def create_tables_in_schema(S, schema_name, metadata):
    # get rid of the password. See about using certificates instead.
    # e = create_engine("postgresql+psycopg2://tgrid:tgrid4all@localhost:5432/tgrid")
    #e = create_engine("sqlite://", module=pysqlite3)
    # connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=LAPTOP-M6BP34C4.local\TGRID4All;UID=tgrid4all;PWD=tgrid"
    # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    # e = create_engine(connection_url)
    # This is a workaround to the problem with schema-scoped names as they are not directly
    # supported in SQLite.
    S.execute(f"ATTACH DATABASE ':memory:' AS {schema_name}")
    # SQLite does not have schemas so map it to None.
    # However, I was not able to get this workaround to work correctly so use the ATTACH DATABASE
    # trick above.
    # xref https://docs.sqlalchemy.org/en/14/core/connections.html#translation-of-schema-names
    # schema_map = {"socrata": None}
    # e.connect().execution_options(schema_translate_map=schema_map)
    # e.execution_options(schema_translate_map=schema_map)
    tables = filter(lambda t: t.schema==schema_name, metadata.tables)
    metadata.create_all(bind=S.bind, tables=tables)
