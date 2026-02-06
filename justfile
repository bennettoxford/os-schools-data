set dotenv-load := true

# extract specific sql into csv, e.g. just extract students
extract name: _vm_only
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

# generate report on real data in Level 4 dir.
report-real:
    uv run generate_report.py $DATA_DIR "Real Data" > $LEVEL4_DIR/real.md
