import os
from urllib.parse import quote

server = os.environ["DB_HOST"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
database = os.environ["DB_NAME"]

pyodbc_conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={user};"
    f"PWD={password};"
)

mssql_conn_str = f"mssql+pymssql://{user}:{quote(password)}@{server}/{database}"
