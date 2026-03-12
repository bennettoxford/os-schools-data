import argparse
import sys
import time
from collections import defaultdict
from datetime import date
from html import escape as html_escape
from pathlib import Path

from sqlalchemy import create_engine, text

import env

NUMERIC_TYPES = {
    "bigint",
    "int",
    "smallint",
    "tinyint",
    "decimal",
    "numeric",
    "float",
    "real",
    "money",
    "smallmoney",
}
DATE_TYPES = {"date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time"}
BOOL_TYPES = {"bit"}
STRING_TYPES = {"char", "nchar", "varchar", "nvarchar", "text", "ntext"}

NUMERIC_MIXED_THRESHOLD = 0.8
SUPPRESS_THRESHOLD = 7
ROUND_BASE = 5
MAX_JACCARD_SCHOOLS = 200

RAW_TABLES = [
    "assessments",
    "attainments",
    "classes",
    "students",
    "teacherClassAllocations",
    "teachers",
]

KNOWN_ID_COLUMNS = {
    "assessments": {"assessmentKey"},
    "attainments": {"attainmentKey", "studentId", "assessmentId", "classId"},
    "classes": {"classKey"},
    "students": {"studentKey"},
    "teacherClassAllocations": {"teacherId", "classId"},
    "teachers": {"teacherKey"},
}

KNOWN_COMBINED_COLUMNS = {
    "attainments": {"score"},
}


def main():
    parser = argparse.ArgumentParser(description="Generate a data report for raw source tables.")
    parser.add_argument("title", nargs="?", default="Raw Source Tables", help="Report title")
    parser.add_argument(
        "output_file",
        nargs="?",
        default="reports/data.html",
        help="Output HTML file path",
    )
    args = parser.parse_args()

    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(env.mssql_conn_str)
    with engine.connect() as conn:
        schema = load_schema_metadata(conn, RAW_TABLES)
        unique_columns = load_unique_index_columns(conn, RAW_TABLES)
        primary_key_columns = load_primary_key_columns(conn, RAW_TABLES)

        body_lines = []
        add_line(body_lines, f"<h1>TED Data Report: {html_escape(args.title)}</h1>")
        add_line(body_lines, f"<p class=\"meta\">Generated on {date.today()}.</p>")
        add_line(body_lines, "<h2>Tables</h2>")

        table_names = [t for t in RAW_TABLES if t in schema]
        if table_names:
            add_line(body_lines, "<ul class=\"table-list\">")
            for table_name in table_names:
                escaped = html_escape(table_name)
                anchor = anchor_id(table_name)
                add_line(body_lines, f"  <li><a href=\"#{anchor}\">{escaped}</a></li>")
            add_line(body_lines, "</ul>")
        else:
            add_line(body_lines, "<p>No tables found.</p>")

        for table_name in RAW_TABLES:
            table_schema = schema.get(table_name)
            if table_schema is None:
                print(f"Skipping {table_name}: table not found", file=sys.stderr)
                continue

            body_lines.append(
                build_table_section(
                    conn,
                    table_name,
                    table_schema,
                    unique_columns.get(table_name, set()),
                    primary_key_columns.get(table_name, set()),
                )
            )

    html = html_template(f"TED Data Report: {args.title}", "\n".join(body_lines))
    output_file.write_text(html, encoding="utf-8")


def build_table_section(conn, table_name, table_schema, unique_columns, primary_key_columns):
    def timed_step(label, func, *args, **kwargs):
        started = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - started
        print(f"  - {label}: {format_duration(elapsed)}", file=sys.stderr)
        return result

    lines = []
    add_line(lines, f"<section class=\"table-section\" id=\"{anchor_id(table_name)}\">")
    table_rows = fetch_table_row_count(conn, table_name)
    add_line(lines, f"  <h2>{html_escape(table_name)} (rows: {format_count(table_rows)})</h2>")

    column_types = {column: data_type for column, data_type in table_schema}
    school_column = find_school_column(table_schema)
    columns = [column for column, _ in table_schema if column.lower() != "rowid"]
    if not columns:
        add_line(lines, "  <p>No columns found (after excluding rowid).</p>")
        add_line(lines, "</section>")
        return "\n".join(lines)

    headers = [
        "Column",
        "Detected type",
        "DB type",
        "Empty values",
        "Distinct values\n(non-empty)",
        "Unique values\n(non-empty)",
        "Flags",
        "Metadata",
    ]

    add_line(lines, "  <div class=\"table-wrap\">")
    add_line(lines, "  <table class=\"data\">")
    add_line(lines, "    <thead>")
    add_line(lines, "      <tr>")
    for header in headers:
        add_line(lines, f"        <th>{html_escape(header)}</th>")
    add_line(lines, "      </tr>")
    add_line(lines, "    </thead>")
    add_line(lines, "    <tbody>")

    for column in columns:
        column_started = time.perf_counter()
        print(f"Processing {table_name}.{column}...", file=sys.stderr)
        data_type = column_types.get(column)
        is_known_combined = column in KNOWN_COMBINED_COLUMNS.get(table_name, set())
        is_primary_key = column in primary_key_columns
        is_known_id = column in KNOWN_ID_COLUMNS.get(table_name, set())
        column_lower = column.lower()
        is_upn = column_lower.endswith("upn")
        is_urn_suffix = column_lower.endswith("urn")
        is_key_suffix = column_lower.endswith("key")
        is_id_suffix = column_lower.endswith("id")
        is_id_candidate = is_primary_key or is_known_id or is_upn or is_urn_suffix or is_key_suffix or is_id_suffix
        is_common_fixed_candidate = column_lower in {"datasource", "importedon"}
        if is_id_candidate:
            summary = timed_step("Summary", fetch_counts_only, conn, table_name, column, data_type)
            non_empty = (summary.get("total_count") or 0) - (summary.get("empty_count") or 0)
            distinct_non_empty = None
            is_fixed = False
            summary.update(
                {
                    "distinct_non_empty": distinct_non_empty,
                    "numeric_count": 0,
                    "date_count": 0,
                    "bool_count": 0,
                }
            )
        elif is_common_fixed_candidate:
            summary = timed_step("Summary", fetch_counts_only, conn, table_name, column, data_type)
            non_empty = (summary.get("total_count") or 0) - (summary.get("empty_count") or 0)
            distinct_probe = timed_step("Distinct probe", fetch_distinct_non_empty_probe, conn, table_name, column, data_type)
            if distinct_probe <= 1:
                distinct_non_empty = distinct_probe
                is_fixed = non_empty > 0 and distinct_non_empty == 1
                summary.update(
                    {
                        "distinct_non_empty": distinct_non_empty,
                        "numeric_count": 0,
                        "date_count": 0,
                        "bool_count": 0,
                    }
                )
            else:
                distinct_non_empty = timed_step("Distinct count", fetch_distinct_non_empty_count, conn, table_name, column, data_type)
                is_fixed = False
                inferred_profile = infer_typed_profile_counts(data_type, non_empty)
                if inferred_profile is None:
                    type_profile = timed_step("Type profile", fetch_type_profile, conn, table_name, column, data_type)
                else:
                    type_profile = inferred_profile
                summary.update({"distinct_non_empty": distinct_non_empty})
                summary.update(type_profile)
        else:
            if is_known_combined:
                summary = timed_step("Summary", fetch_combined_summary, conn, table_name, column, data_type)
            else:
                summary = timed_step("Summary", fetch_column_summary, conn, table_name, column, data_type)
            non_empty = (summary.get("total_count") or 0) - (summary.get("empty_count") or 0)
            distinct_non_empty = summary.get("distinct_non_empty") or 0
            is_fixed = non_empty > 0 and distinct_non_empty == 1

        is_unique_by_counts = non_empty > 0 and distinct_non_empty is not None and distinct_non_empty == non_empty
        is_unique_by_index = column in unique_columns
        is_unique = is_primary_key or is_unique_by_counts or is_unique_by_index
        is_id = is_id_candidate
        numeric_count = summary.get("numeric_count") or 0
        classification = classify_column(
            data_type,
            summary,
            is_id=is_id,
            is_fixed=is_fixed,
            force_combined=is_known_combined,
        )

        flags = []
        if is_primary_key:
            flags.append("pk")
        if is_known_id and not is_primary_key:
            flags.append("known-id")
        if is_upn:
            flags.append("upn")
        if is_urn_suffix:
            flags.append("urn-suffix")
        if is_key_suffix:
            flags.append("key-suffix")
        if is_id_suffix:
            flags.append("id-suffix")
        flags_display = "\n".join(flags) if flags else "-"

        metadata_items = []

        if classification == "fixed":
            add_meta_item(
                metadata_items,
                "Value",
                format_value(timed_step("Fixed value", fetch_fixed_value, conn, table_name, column)),
            )
        elif classification == "id":
            min_val, max_val = timed_step("ID min/max", get_min_max_for_type, conn, table_name, column, data_type)
            add_meta_item(metadata_items, "Min", min_val)
            add_meta_item(metadata_items, "Max", max_val)
        elif classification in {"numeric", "combined"}:
            stats = timed_step("Numeric stats", fetch_numeric_stats, conn, table_name, column, data_type)
            add_meta_item(metadata_items, "Min", format_value(stats.get("min_value")))
            add_meta_item(metadata_items, "Max", format_value(stats.get("max_value")))
            add_meta_item(metadata_items, "Mean", format_value(stats.get("mean_value")))
            add_meta_item(metadata_items, "Median", format_value(stats.get("median_value")))
            if classification == "combined":
                add_meta_item(metadata_items, "Numeric coverage", format_ratio(numeric_count, non_empty))
                add_meta_item(
                    metadata_items,
                    "Non-numeric values",
                    format_count(max(non_empty - numeric_count, 0)),
                )
                non_numeric_distinct = timed_step(
                    "Non-numeric distinct count",
                    fetch_distinct_count,
                    conn,
                    table_name,
                    column,
                    non_numeric_only=True,
                )
                add_meta_item(
                    metadata_items,
                    "Non-numeric distinct values",
                    format_distinct_count(non_numeric_distinct),
                )
                if has_school_context(table_name, school_column):
                    avg_pairwise_jaccard = timed_step(
                        "Non-numeric pairwise Jaccard",
                        fetch_avg_pairwise_jaccard,
                        conn,
                        table_name,
                        column,
                        school_column,
                        non_numeric_only=True,
                    )
                    add_meta_item(
                        metadata_items,
                        "Avg pairwise Jaccard (non-numeric by school)",
                        format_percent(avg_pairwise_jaccard),
                        tooltip=(
                            "Average overlap of distinct value sets across all school pairs. "
                            "0% means no shared values between schools; 100% means identical sets."
                        ),
                    )
                string_values = timed_step(
                    "String distribution data",
                    fetch_categorical_values,
                    conn,
                    table_name,
                    column,
                    non_numeric_only=True,
                    school_column=school_column,
                )
                string_dist_cell = (
                    "<details><summary>Show string distribution</summary>"
                    + build_distribution_table(string_values)
                    + "</details>"
                )
                add_meta_item(metadata_items, "String distribution", string_dist_cell)
        elif classification == "date":
            stats = timed_step("Date stats", fetch_date_stats, conn, table_name, column, data_type)
            add_meta_item(metadata_items, "Min", format_value(stats.get("min_value")))
            add_meta_item(metadata_items, "Max", format_value(stats.get("max_value")))
        elif classification == "categorical":
            if has_school_context(table_name, school_column):
                avg_pairwise_jaccard = timed_step(
                    "Pairwise Jaccard",
                    fetch_avg_pairwise_jaccard,
                    conn,
                    table_name,
                    column,
                    school_column,
                )
                add_meta_item(
                    metadata_items,
                    "Avg pairwise Jaccard (by school)",
                    format_percent(avg_pairwise_jaccard),
                    tooltip=(
                        "Average overlap of distinct value sets across all school pairs. "
                        "0% means no shared values between schools; 100% means identical sets."
                    ),
                )
            values = timed_step(
                "Distribution data",
                fetch_categorical_values,
                conn,
                table_name,
                column,
                school_column=school_column,
            )
            distribution_cell = "<details><summary>Show</summary>" + build_distribution_table(values) + "</details>"
            add_meta_item(metadata_items, "Distribution", distribution_cell)

        metadata_html = (
            "<ul class=\"meta-list\">"
            + "".join(metadata_items)
            + "</ul>"
            if metadata_items
            else "-"
        )

        row_class = " class=\"is-empty\"" if classification == "empty" else ""
        add_line(lines, f"      <tr{row_class}>")
        add_line(lines, f"        <td>{html_escape(column)}</td>")
        add_line(lines, f"        <td>{html_escape(classification)}</td>")
        add_line(lines, f"        <td>{html_escape(data_type or '-') }</td>")
        empty_count = summary.get("empty_count") or 0
        total_count = summary.get("total_count") or 0
        add_line(
            lines,
            f"        <td>{format_count(empty_count)} ({format_percent_of_total(empty_count, total_count)})</td>",
        )
        add_line(lines, f"        <td>{format_distinct_count(summary.get('distinct_non_empty'))}</td>")
        add_line(lines, f"        <td>{'yes' if is_unique else 'no'}</td>")
        add_line(lines, f"        <td class=\"flags\">{html_escape(flags_display)}</td>")
        add_line(lines, f"        <td>{metadata_html}</td>")
        add_line(lines, "      </tr>")
        print(
            f"Finished {table_name}.{column} in {format_duration(time.perf_counter() - column_started)}",
            file=sys.stderr,
        )

    add_line(lines, "    </tbody>")
    add_line(lines, "  </table>")
    add_line(lines, "  </div>")
    add_line(lines, "</section>")
    return "\n".join(lines)


def anchor_id(name):
    return "table-" + "".join(ch if ch.isalnum() else "-" for ch in name.lower()).strip("-")


def find_school_column(table_schema):
    for column, _ in table_schema:
        if column.replace("_", "").lower() == "schoolid":
            return column
    return None


def has_school_context(table_name, school_column):
    return school_column is not None or table_name in {"attainments", "assessments", "teacherClassAllocations"}


def resolve_school_source(table, school_column):
    table_q = quote_ident(table)
    if school_column:
        school_q = quote_ident(school_column)
        return (
            f"{table_q} AS src",
            f"CONVERT(NVARCHAR(MAX), src.{school_q})",
        )
    if table == "attainments":
        return (
            f"{table_q} AS src "
            "LEFT JOIN [students] AS stu ON stu.[studentKey] = src.[studentId]",
            "CONVERT(NVARCHAR(MAX), stu.[schoolId])",
        )
    if table == "assessments":
        return (
            f"{table_q} AS src "
            "LEFT JOIN [attainments] AS at ON at.[assessmentId] = src.[assessmentKey] "
            "LEFT JOIN [students] AS stu ON stu.[studentKey] = at.[studentId]",
            "CONVERT(NVARCHAR(MAX), stu.[schoolId])",
        )
    if table == "teacherClassAllocations":
        return (
            f"{table_q} AS src "
            "LEFT JOIN [classes] AS cls ON cls.[classKey] = src.[classId]",
            "CONVERT(NVARCHAR(MAX), cls.[schoolId])",
        )
    return None, None


def get_min_max_for_type(conn, table, column, data_type):
    data_type = (data_type or "").lower()
    if data_type in NUMERIC_TYPES:
        stats = fetch_numeric_stats(conn, table, column, data_type)
        return format_value(stats.get("min_value")), format_value(stats.get("max_value"))
    if data_type in DATE_TYPES:
        stats = fetch_date_stats(conn, table, column, data_type)
        return format_value(stats.get("min_value")), format_value(stats.get("max_value"))
    stats = fetch_string_min_max(conn, table, column)
    return format_value(stats.get("min_value")), format_value(stats.get("max_value"))


def build_distribution_table(values):
    if not values:
        return "<p>-</p>"

    max_count = max((row[1] for row in values), default=0)
    rows = []
    rows.append("<table class=\"dist\">")
    rows.append(
        "  <thead><tr><th>Value</th><th class=\"count\">Count</th><th class=\"count\">Schools</th><th>Distribution</th></tr></thead>"
    )
    rows.append("  <tbody>")

    for row in values:
        value = row[0]
        count = row[1]
        school_count = row[2] if len(row) > 2 else None
        label = "(empty)" if value is None else str(value)
        label_html = html_escape(" ".join(label.split()))
        if max_count > 0:
            pct = (count / max_count) * 100
        else:
            pct = 0
        count_display = format_count(count)
        school_count_display = format_distinct_count(school_count)
        rows.append(
            "    <tr>"
            f"<td class=\"value\">{label_html}</td>"
            f"<td class=\"count\">{count_display}</td>"
            f"<td class=\"count\">{school_count_display}</td>"
            f"<td><div class=\"bar-wrap\"><div class=\"bar\" style=\"width: {pct:.2f}%\"></div></div></td>"
            "</tr>"
        )

    rows.append("  </tbody>")
    rows.append("</table>")
    return "\n".join(rows)


def add_line(lines, text=""):
    lines.append(text)


def add_meta_item(items, label, value_html, tooltip=None):
    tooltip_attr = f" title=\"{html_escape(tooltip)}\"" if tooltip else ""
    items.append(f"<li><span class=\"meta-label\"{tooltip_attr}>{html_escape(label)}:</span> {value_html}</li>")


def html_template(title, body):
    title_escaped = html_escape(title)
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{title_escaped}</title>
  <style>
    :root {{
      color-scheme: light;
      --text: #1f2933;
      --muted: #6b7280;
      --border: #e5e7eb;
      --bar: #475569;
      --bar-bg: #e2e8f0;
      --link: #0b57d0;
    }}
    body {{
      font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
      color: var(--text);
      margin: 24px;
      line-height: 1.45;
    }}
    a {{ color: var(--link); }}
    .meta {{ color: var(--muted); margin-top: -8px; }}
    .table-list {{ columns: 2; column-gap: 32px; }}
    .table-list li {{ break-inside: avoid; }}
    .table-section {{
      border-top: 2px solid var(--border);
      padding-top: 16px;
      margin-top: 20px;
    }}
    .table-wrap {{
      overflow-x: auto;
      margin-top: 12px;
    }}
    table.data {{
      width: 100%;
      border-collapse: collapse;
      min-width: 1200px;
    }}
    table.data th,
    table.data td {{
      border-bottom: 1px solid var(--border);
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
      font-size: 0.92em;
    }}
    table.data th {{
      position: sticky;
      top: 0;
      background: #fff;
      z-index: 1;
      white-space: pre-line;
    }}
    table.data td {{
      word-break: break-word;
    }}
    table.data td.flags {{
      white-space: pre-line;
    }}
    table.data tr.is-empty td {{
      color: var(--muted);
    }}
    .meta-list {{
      list-style: none;
      padding-left: 0;
      margin: 0;
      display: grid;
      gap: 4px;
    }}
    .meta-label {{
      font-weight: 600;
    }}
    table.dist {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
    }}
    table.dist th,
    table.dist td {{
      border-bottom: 1px solid var(--border);
      padding: 4px 6px;
      text-align: left;
      vertical-align: top;
      font-size: 0.9em;
    }}
    table.dist th.count,
    table.dist td.count {{
      text-align: right;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }}
    table.dist td.value {{
      word-break: break-word;
      max-width: 420px;
    }}
    details summary {{
      cursor: pointer;
      color: var(--link);
    }}
    .bar-wrap {{
      background: var(--bar-bg);
      height: 10px;
      border-radius: 6px;
      overflow: hidden;
    }}
    .bar {{
      background: var(--bar);
      height: 100%;
    }}
    @media (max-width: 900px) {{
      .table-list {{ columns: 1; }}
      table.data {{ min-width: 900px; }}
      table.dist td.value {{ max-width: 240px; }}
    }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def load_schema_metadata(conn, table_names):
    if not table_names:
        return {}
    table_list = ", ".join(f"'{name}'" for name in table_names)
    query = f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME IN ({table_list})
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    result = conn.execute(text(query))
    schema = defaultdict(list)
    for row in result:
        mapping = row._mapping if hasattr(row, "_mapping") else row
        table = mapping["TABLE_NAME"]
        column = mapping["COLUMN_NAME"]
        data_type = mapping["DATA_TYPE"]
        schema[table].append((column, data_type))
    return schema


def load_unique_index_columns(conn, table_names):
    if not table_names:
        return {}
    table_list = ", ".join(f"'{name}'" for name in table_names)
    query = f"""
        WITH key_counts AS (
            SELECT
                i.object_id,
                i.index_id,
                COUNT(CASE WHEN ic.is_included_column = 0 THEN 1 END) AS key_count
            FROM sys.indexes AS i
            JOIN sys.index_columns AS ic
                ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            WHERE i.is_unique = 1
            GROUP BY i.object_id, i.index_id
        )
        SELECT
            t.name AS table_name,
            c.name AS column_name
        FROM sys.indexes AS i
        JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns AS c
            ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.tables AS t
            ON t.object_id = i.object_id
        JOIN key_counts AS kc
            ON kc.object_id = i.object_id AND kc.index_id = i.index_id
        WHERE i.is_unique = 1
            AND ic.is_included_column = 0
            AND kc.key_count = 1
            AND t.name IN ({table_list})
    """
    result = conn.execute(text(query))
    unique_columns = defaultdict(set)
    for row in result:
        mapping = row._mapping if hasattr(row, "_mapping") else row
        unique_columns[mapping["table_name"]].add(mapping["column_name"])
    return unique_columns


def load_primary_key_columns(conn, table_names):
    if not table_names:
        return {}
    table_list = ", ".join(f"'{name}'" for name in table_names)
    query = f"""
        SELECT
            t.name AS table_name,
            c.name AS column_name
        FROM sys.indexes AS i
        JOIN sys.index_columns AS ic
            ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns AS c
            ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.tables AS t
            ON t.object_id = i.object_id
        WHERE i.is_primary_key = 1
            AND t.name IN ({table_list})
    """
    result = conn.execute(text(query))
    primary_keys = defaultdict(set)
    for row in result:
        mapping = row._mapping if hasattr(row, "_mapping") else row
        primary_keys[mapping["table_name"]].add(mapping["column_name"])
    return primary_keys


def fetch_counts_only(conn, table, column, data_type):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()

    if data_type in STRING_TYPES:
        col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
        query = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {empty_expr} THEN 1 ELSE 0 END) AS empty_count
            FROM {table_q}
        """
    else:
        query = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {col_q} IS NULL THEN 1 ELSE 0 END) AS empty_count
            FROM {table_q}
        """

    row = conn.execute(text(query)).fetchone()
    return row_to_dict(row)


def fetch_distinct_non_empty_probe(conn, table, column, data_type):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()

    if data_type in STRING_TYPES:
        col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
        value_expr = col_expr
    else:
        empty_expr = f"({col_q} IS NULL)"
        value_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"

    query = f"""
        SELECT COUNT(*) AS distinct_probe
        FROM (
            SELECT DISTINCT TOP 2 {value_expr} AS value
            FROM {table_q}
            WHERE NOT {empty_expr}
        ) AS d
    """
    row = conn.execute(text(query)).fetchone()
    mapping = row._mapping if hasattr(row, "_mapping") else row
    return (mapping.get("distinct_probe") or 0) if mapping else 0


def fetch_distinct_non_empty_count(conn, table, column, data_type):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()

    if data_type in STRING_TYPES:
        col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
        value_expr = col_expr
    else:
        empty_expr = f"({col_q} IS NULL)"
        value_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"

    query = f"""
        SELECT COUNT(DISTINCT CASE WHEN {empty_expr} THEN NULL ELSE {value_expr} END) AS distinct_non_empty
        FROM {table_q}
    """
    row = conn.execute(text(query)).fetchone()
    mapping = row._mapping if hasattr(row, "_mapping") else row
    return (mapping.get("distinct_non_empty") or 0) if mapping else 0


def infer_typed_profile_counts(data_type, non_empty):
    data_type = (data_type or "").lower()
    if non_empty <= 0:
        return {"numeric_count": 0, "date_count": 0, "bool_count": 0}
    if data_type in NUMERIC_TYPES:
        return {"numeric_count": non_empty, "date_count": 0, "bool_count": 0}
    if data_type in DATE_TYPES:
        return {"numeric_count": 0, "date_count": non_empty, "bool_count": 0}
    if data_type in BOOL_TYPES:
        return {"numeric_count": 0, "date_count": 0, "bool_count": non_empty}
    return None


def fetch_type_profile(conn, table, column, data_type):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()
    col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"

    if data_type in STRING_TYPES:
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
    else:
        empty_expr = f"({col_q} IS NULL)"

    query = f"""
        SELECT
            SUM(CASE WHEN NOT {empty_expr} AND TRY_CAST({col_expr} AS FLOAT) IS NOT NULL THEN 1 ELSE 0 END) AS numeric_count,
            SUM(CASE WHEN NOT {empty_expr} AND TRY_CONVERT(date, {col_expr}) IS NOT NULL THEN 1 ELSE 0 END) AS date_count,
            SUM(
                CASE
                    WHEN NOT {empty_expr} AND LOWER(LTRIM(RTRIM({col_expr}))) IN ('t','f','true','false','0','1','yes','no','y','n')
                    THEN 1
                    ELSE 0
                END
            ) AS bool_count
        FROM {table_q}
    """
    row = conn.execute(text(query)).fetchone()
    return row_to_dict(row)


def fetch_combined_summary(conn, table, column, data_type):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()
    col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
    if data_type in STRING_TYPES:
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
    else:
        empty_expr = f"({col_q} IS NULL)"

    query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN {empty_expr} THEN 1 ELSE 0 END) AS empty_count,
            COUNT(DISTINCT CASE WHEN {empty_expr} THEN NULL ELSE {col_expr} END) AS distinct_non_empty,
            SUM(CASE WHEN NOT {empty_expr} AND TRY_CAST({col_expr} AS FLOAT) IS NOT NULL THEN 1 ELSE 0 END) AS numeric_count,
            CAST(0 AS BIGINT) AS date_count,
            CAST(0 AS BIGINT) AS bool_count
        FROM {table_q}
    """

    row = conn.execute(text(query)).fetchone()
    return row_to_dict(row)


def fetch_column_summary(conn, table, column, data_type):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()

    if data_type in STRING_TYPES:
        col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
        query = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {empty_expr} THEN 1 ELSE 0 END) AS empty_count,
                COUNT(DISTINCT CASE WHEN {empty_expr} THEN NULL ELSE {col_expr} END) AS distinct_non_empty,
                SUM(CASE WHEN NOT {empty_expr} AND TRY_CAST({col_expr} AS FLOAT) IS NOT NULL THEN 1 ELSE 0 END) AS numeric_count,
                SUM(CASE WHEN NOT {empty_expr} AND TRY_CONVERT(date, {col_expr}) IS NOT NULL THEN 1 ELSE 0 END) AS date_count,
                SUM(
                    CASE
                        WHEN NOT {empty_expr} AND LOWER(LTRIM(RTRIM({col_expr}))) IN ('t','f','true','false','0','1','yes','no','y','n')
                        THEN 1
                        ELSE 0
                    END
                ) AS bool_count
            FROM {table_q}
        """
    elif data_type in NUMERIC_TYPES:
        query = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {col_q} IS NULL THEN 1 ELSE 0 END) AS empty_count,
                COUNT(DISTINCT {col_q}) AS distinct_non_empty,
                SUM(CASE WHEN {col_q} IS NULL THEN 0 ELSE 1 END) AS numeric_count,
                CAST(0 AS BIGINT) AS date_count,
                CAST(0 AS BIGINT) AS bool_count
            FROM {table_q}
        """
    elif data_type in DATE_TYPES:
        query = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {col_q} IS NULL THEN 1 ELSE 0 END) AS empty_count,
                COUNT(DISTINCT {col_q}) AS distinct_non_empty,
                CAST(0 AS BIGINT) AS numeric_count,
                SUM(CASE WHEN {col_q} IS NULL THEN 0 ELSE 1 END) AS date_count,
                CAST(0 AS BIGINT) AS bool_count
            FROM {table_q}
        """
    elif data_type in BOOL_TYPES:
        query = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {col_q} IS NULL THEN 1 ELSE 0 END) AS empty_count,
                COUNT(DISTINCT {col_q}) AS distinct_non_empty,
                CAST(0 AS BIGINT) AS numeric_count,
                CAST(0 AS BIGINT) AS date_count,
                SUM(CASE WHEN {col_q} IS NULL THEN 0 ELSE 1 END) AS bool_count
            FROM {table_q}
        """
    else:
        col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
        empty_expr = f"({col_q} IS NULL)"
        query = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {empty_expr} THEN 1 ELSE 0 END) AS empty_count,
                COUNT(DISTINCT CASE WHEN {empty_expr} THEN NULL ELSE {col_expr} END) AS distinct_non_empty,
                SUM(CASE WHEN NOT {empty_expr} AND TRY_CAST({col_expr} AS FLOAT) IS NOT NULL THEN 1 ELSE 0 END) AS numeric_count,
                SUM(CASE WHEN NOT {empty_expr} AND TRY_CONVERT(date, {col_expr}) IS NOT NULL THEN 1 ELSE 0 END) AS date_count,
                SUM(
                    CASE
                        WHEN NOT {empty_expr} AND LOWER(LTRIM(RTRIM({col_expr}))) IN ('t','f','true','false','0','1','yes','no','y','n')
                        THEN 1
                        ELSE 0
                    END
                ) AS bool_count
            FROM {table_q}
        """

    row = conn.execute(text(query)).fetchone()
    return row_to_dict(row)


def fetch_table_row_count(conn, table):
    table_q = quote_ident(table)
    query = f"SELECT COUNT(*) AS total_count FROM {table_q}"
    row = conn.execute(text(query)).fetchone()
    if row is None:
        return 0
    mapping = row._mapping if hasattr(row, "_mapping") else row
    return mapping.get("total_count") or 0


def fetch_numeric_stats(conn, table, column, data_type=None):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()

    if data_type in NUMERIC_TYPES:
        query = f"""
            WITH vals AS (
                SELECT CAST({col_q} AS FLOAT) AS val
                FROM {table_q}
                WHERE {col_q} IS NOT NULL
            ),
            stats AS (
                SELECT
                    MIN(val) OVER () AS min_value,
                    MAX(val) OVER () AS max_value,
                    AVG(val) OVER () AS mean_value,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) OVER () AS median_value
                FROM vals
            )
            SELECT TOP 1 min_value, max_value, mean_value, median_value
            FROM stats
        """
    else:
        col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
        query = f"""
            WITH vals AS (
                SELECT TRY_CAST({col_expr} AS FLOAT) AS val
                FROM {table_q}
                WHERE NOT {empty_expr}
            ),
            stats AS (
                SELECT
                    MIN(val) OVER () AS min_value,
                    MAX(val) OVER () AS max_value,
                    AVG(val) OVER () AS mean_value,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) OVER () AS median_value
                FROM vals
                WHERE val IS NOT NULL
            )
            SELECT TOP 1 min_value, max_value, mean_value, median_value
            FROM stats
        """

    row = conn.execute(text(query)).fetchone()
    return row_to_dict(row)


def fetch_date_stats(conn, table, column, data_type=None):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    data_type = (data_type or "").lower()
    if data_type in DATE_TYPES:
        query = f"""
            SELECT
                MIN(CAST({col_q} AS date)) AS min_value,
                MAX(CAST({col_q} AS date)) AS max_value
            FROM {table_q}
            WHERE {col_q} IS NOT NULL
        """
    else:
        col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
        empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
        query = f"""
            SELECT
                MIN(val) AS min_value,
                MAX(val) AS max_value
            FROM (
                SELECT TRY_CONVERT(date, {col_expr}) AS val
                FROM {table_q}
                WHERE NOT {empty_expr}
            ) AS valueset
            WHERE val IS NOT NULL
        """

    row = conn.execute(text(query)).fetchone()
    return row_to_dict(row)


def fetch_categorical_values(conn, table, column, non_numeric_only=False, school_column=None):
    col_q = quote_ident(column)
    col_expr = f"CONVERT(NVARCHAR(MAX), src.{col_q})"
    empty_expr = f"(src.{col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
    non_numeric_filter = ""
    if non_numeric_only:
        non_numeric_filter = f" AND TRY_CAST({col_expr} AS FLOAT) IS NULL"
    table_q = quote_ident(table)
    source_from = f"{table_q} AS src"
    school_from, school_expr = resolve_school_source(table, school_column)
    if not school_from:
        school_from = source_from

    base_query = f"""
        WITH value_counts AS (
            SELECT
                {col_expr} AS value,
                COUNT(*) AS value_count
            FROM {source_from}
            WHERE NOT {empty_expr}{non_numeric_filter}
            GROUP BY {col_expr}
        )
    """

    if school_expr:
        school_empty = f"({school_expr} IS NULL OR LTRIM(RTRIM({school_expr})) = '')"
        query = (
            base_query
            + f""",
        school_counts AS (
            SELECT sv.value, COUNT(DISTINCT sv.school_id) AS school_count
            FROM (
                SELECT DISTINCT
                    {col_expr} AS value,
                    {school_expr} AS school_id
                FROM {school_from}
                WHERE NOT {empty_expr}{non_numeric_filter}
                  AND NOT {school_empty}
            ) AS sv
            GROUP BY sv.value
        )
        SELECT
            vc.value,
            vc.value_count,
            sc.school_count
        FROM value_counts vc
        LEFT JOIN school_counts sc ON sc.value = vc.value
        ORDER BY vc.value_count DESC, vc.value
        """
        )
    else:
        query = (
            base_query
            + """
        SELECT
            vc.value,
            vc.value_count,
            NULL AS school_count
        FROM value_counts vc
        ORDER BY vc.value_count DESC, vc.value
        """
        )

    result = conn.execute(text(query))
    values = []
    for row in result:
        mapping = row._mapping if hasattr(row, "_mapping") else row
        value = mapping["value"]
        count = mapping["value_count"]
        school_count = mapping.get("school_count") if hasattr(mapping, "get") else None
        values.append((value, count, school_count))
    return values


def fetch_distinct_count(conn, table, column, non_numeric_only=False):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
    empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
    non_numeric_filter = ""
    if non_numeric_only:
        non_numeric_filter = f" AND TRY_CAST({col_expr} AS FLOAT) IS NULL"

    query = f"""
        SELECT COUNT(DISTINCT {col_expr}) AS distinct_count
        FROM {table_q}
        WHERE NOT {empty_expr}{non_numeric_filter}
    """
    row = conn.execute(text(query)).fetchone()
    if row is None:
        return 0
    mapping = row._mapping if hasattr(row, "_mapping") else row
    return mapping.get("distinct_count") or 0


def fetch_avg_pairwise_jaccard(conn, table, column, school_column, non_numeric_only=False):
    col_q = quote_ident(column)
    col_expr = f"CONVERT(NVARCHAR(MAX), src.{col_q})"
    value_empty = f"(src.{col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"
    non_numeric_filter = ""
    if non_numeric_only:
        non_numeric_filter = f" AND TRY_CAST({col_expr} AS FLOAT) IS NULL"
    school_from, school_expr = resolve_school_source(table, school_column)
    if not school_from or not school_expr:
        return None
    school_empty = f"({school_expr} IS NULL OR LTRIM(RTRIM({school_expr})) = '')"
    materialize_query = f"""
        DROP TABLE IF EXISTS #school_values;
        SELECT DISTINCT
            CONVERT(NVARCHAR(255), {school_expr}) AS school_id,
            {col_expr} AS value,
            CHECKSUM({col_expr}) AS value_checksum
        INTO #school_values
        FROM {school_from}
        WHERE NOT {school_empty} AND NOT {value_empty}{non_numeric_filter};
    """

    try:
        conn.execute(text(materialize_query))
        conn.execute(text("CREATE INDEX ix_school_values_school ON #school_values (school_id)"))
        conn.execute(
            text("CREATE INDEX ix_school_values_checksum_school ON #school_values (value_checksum, school_id)")
        )

        school_count_row = conn.execute(
            text("SELECT COUNT(DISTINCT school_id) AS school_count FROM #school_values")
        ).fetchone()
        school_count_map = school_count_row._mapping if hasattr(school_count_row, "_mapping") else school_count_row
        school_count = (school_count_map.get("school_count") or 0) if school_count_map else 0
        if school_count < 2:
            return None
        if school_count > MAX_JACCARD_SCHOOLS:
            return f"Skipped ({school_count} schools)"

        query = """
            WITH school_sizes AS (
                SELECT school_id, COUNT(*) AS n_values
                FROM #school_values
                GROUP BY school_id
            ),
            school_pairs AS (
                SELECT a.school_id AS s1, b.school_id AS s2, a.n_values AS n1, b.n_values AS n2
                FROM school_sizes a
                JOIN school_sizes b ON a.school_id < b.school_id
            ),
            pair_intersections AS (
                SELECT a.school_id AS s1, b.school_id AS s2, COUNT(*) AS intersect_count
                FROM #school_values a
                JOIN #school_values b
                  ON a.value_checksum = b.value_checksum
                 AND a.value = b.value
                 AND a.school_id < b.school_id
                GROUP BY a.school_id, b.school_id
            ),
            pair_stats AS (
                SELECT
                    p.s1,
                    p.s2,
                    COALESCE(i.intersect_count, 0) AS intersect_count,
                    p.n1 + p.n2 - COALESCE(i.intersect_count, 0) AS union_count
                FROM school_pairs p
                LEFT JOIN pair_intersections i
                  ON i.s1 = p.s1 AND i.s2 = p.s2
            )
            SELECT AVG(CAST(intersect_count AS FLOAT) / NULLIF(union_count, 0)) AS avg_jaccard
            FROM pair_stats
        """

        row = conn.execute(text(query)).fetchone()
        if row is None:
            return None
        mapping = row._mapping if hasattr(row, "_mapping") else row
        return mapping.get("avg_jaccard")
    finally:
        conn.execute(text("DROP TABLE IF EXISTS #school_values;"))


def classify_column(data_type, summary, is_id=False, is_fixed=False, force_combined=False):
    total = summary.get("total_count") or 0
    empty = summary.get("empty_count") or 0
    non_empty = max(total - empty, 0)
    if non_empty == 0:
        return "empty"

    if is_fixed:
        return "fixed"
    if is_id:
        return "id"
    data_type = (data_type or "").lower()
    if data_type in BOOL_TYPES:
        return "bool"
    if data_type in DATE_TYPES:
        return "date"
    if data_type in NUMERIC_TYPES:
        return "numeric"

    numeric_count = summary.get("numeric_count") or 0
    date_count = summary.get("date_count") or 0
    bool_count = summary.get("bool_count") or 0
    has_mixed_numeric = 0 < numeric_count < non_empty

    if bool_count == non_empty:
        return "bool"
    if date_count == non_empty:
        return "date"
    if numeric_count == non_empty:
        return "numeric"

    if force_combined and has_mixed_numeric:
        return "combined"

    if has_mixed_numeric and (numeric_count / non_empty) >= NUMERIC_MIXED_THRESHOLD:
        return "combined"

    if bool_count / non_empty >= 0.98:
        return "bool"
    if date_count / non_empty >= 0.98:
        return "date"
    if numeric_count / non_empty >= 0.98:
        return "numeric"

    return "categorical"


def format_ratio(numerator, denominator):
    if denominator <= 0:
        return "0%"
    return f"{(numerator / denominator) * 100:.1f}%"


def format_duration(seconds):
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    remainder = seconds - (minutes * 60)
    return f"{minutes}m {remainder:.1f}s"


def format_percent(value):
    if value is None:
        return "-"
    if isinstance(value, str):
        return html_escape(value)
    return f"{value * 100:.1f}%"


def format_percent_of_total(count, total):
    if total <= 0:
        return "0.0%"
    return f"{(count / total) * 100:.1f}%"


def fetch_fixed_value(conn, table, column):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
    empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"

    query = f"""
        SELECT TOP 1 {col_expr} AS value
        FROM {table_q}
        WHERE NOT {empty_expr}
    """
    row = conn.execute(text(query)).fetchone()
    if row is None:
        return "(empty)"
    mapping = row._mapping if hasattr(row, "_mapping") else row
    return mapping.get("value") if hasattr(mapping, "get") else mapping[0]


def fetch_string_min_max(conn, table, column):
    table_q = quote_ident(table)
    col_q = quote_ident(column)
    col_expr = f"CONVERT(NVARCHAR(MAX), {col_q})"
    empty_expr = f"({col_q} IS NULL OR LTRIM(RTRIM({col_expr})) = '')"

    query = f"""
        SELECT
            MIN(val) AS min_value,
            MAX(val) AS max_value
        FROM (
            SELECT {col_expr} AS val
            FROM {table_q}
            WHERE NOT {empty_expr}
        ) AS valueset
    """

    row = conn.execute(text(query)).fetchone()
    return row_to_dict(row)


def format_value(value):
    if value is None:
        return "-"
    return html_escape(str(value))


def format_count(value):
    if value is None:
        return "-"
    try:
        count = int(value)
    except (TypeError, ValueError):
        return html_escape(str(value))
    if count == 0:
        return "0"
    if count < SUPPRESS_THRESHOLD:
        return html_escape(f"<{SUPPRESS_THRESHOLD}")
    rounded = int(ROUND_BASE * round(count / ROUND_BASE))
    return str(rounded)


def format_distinct_count(value):
    if value is None:
        return "-"
    try:
        count = int(value)
    except (TypeError, ValueError):
        return html_escape(str(value))
    return str(count)


def quote_ident(name):
    return f"[{name.replace(']', ']]')}]"


def row_to_dict(row):
    if row is None:
        return {}
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


if __name__ == "__main__":
    main()
