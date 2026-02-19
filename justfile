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

# generate sythentic data
synthetic-data dir="synthetic-data":
    uv run generate_synthetic_data.py "{{ dir }}"

# generate report on synthetic data
report-synthetic:
    uv run generate_report.py synthetic-data "Synthetic Data" > reports/synthetic.md
    PYTHONWARNINGS=ignore::SyntaxWarning uvx --from ghmdlib ghmd --offline --embed-css reports/synthetic.md

# generate report on real data in Level 4 dir.
report-real:
    uv run generate_report.py $DATA_DIR "Real Data" > $LEVEL4_DIR/real.md
    PYTHONWARNINGS=ignore::SyntaxWarning uvx --from ghmdlib ghmd --offline --embed-css $LEVEL4_DIR/real.md

# generate report on raw source tables
report-raw:
    uv run --with sqlalchemy --with pymssql python generate_data_report.py "Raw Source Tables" $LEVEL4_DIR/data.html
