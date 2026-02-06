from pathlib import Path
import sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
import env

engine = create_engine(env.mssql_conn_str)

path = Path(sys.argv[1])
if path.exists():
    sql = path.read_text()
else:
    sql = sys.argv[1]

try:
    df = pd.read_sql(sql, engine)
except (OperationalError, ProgrammingError) as e:
    print(e.orig.args[1].decode().strip())
    sys.exit(1)

if len(sys.argv) == 3:
    df.to_csv(sys.argv[2], index=False)
else:
    print(df.to_string(index=False))
