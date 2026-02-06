set dotenv-load := true

# extract specific sql into csv, e.g. just extract students
extract name:
    #!/bin/bash
    set -euo pipefail
    test -f "sql/{{name }}.sql"
    echo "Extacting sql/{{ name }}.sql into $DATA_DIR/{{ name }}.csv"
    uv run --with sqlalchemy --with pymssql --with pandas python extract.py sql/{{ name }}.sql $DATA_DIR/{{ name }}.csv


# extract all sql query to csv
extract-all: (extract "teachers") (extract "students") (extract "results")

# run the mssql shell
mssql:
    uv run mssql-cli.py
