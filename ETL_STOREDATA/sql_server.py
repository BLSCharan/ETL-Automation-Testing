from sqlalchemy import create_engine
import json
from pathlib import Path


def get_sql_server_engine():
    base_path = Path(__file__).resolve().parent
    config_path = base_path / "db_config.json"

    with open(config_path) as f:
        config = json.load(f)

    sql_config = config["sql_server"]

    connection_string = (
        f"mssql+pyodbc://@{sql_config['server']}/{sql_config['database']}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )

    engine = create_engine(connection_string)
    return engine