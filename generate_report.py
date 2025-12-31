import csv
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from statistics import mean, median, stdev

SUPPRESS_THRESHOLD = 5

LETTER_GRADES = set("ABCDEU") | {"A*"}
NUMBER_GRADES = {"1", "11", "2", "22", "3", "33", "4", "44", "5", "55", "6", "66", "7", "77", "8", "88", "9", "99", "U"}
KS4_NVQ_GRADES = {"D1", "D2", "P1", "P2", "U"}
KS5_NVQ_GRADES = {"D*", "D*D*D*", "D*D*D", "D*DD", "DDD", "DDM", "MMM", "PPP", "U"}
PLUS_MINUS_SCORES = {"--", "-", "=", "+", "++"}


def main():
    if len(sys.argv) != 3:
        print("Usage: python generate_report.py [path/to/data] [title]")

    input_dir = Path(sys.argv[1])
    title = sys.argv[2]

    print(f"# TED Data Report: {title}")
    print()
    print(f"Generated on {date.today()}.")
    print()

    write_student_section(input_dir)
    print()
    write_teacher_section(input_dir)
    print()
    write_results_section(input_dir)


def write_student_section(input_dir):
    rows, fieldnames = load_csv(input_dir / "students.csv")

    print("## Students")
    print()

    total_students = len(rows)
    missing_rows = sum(1 for row in rows if any(not row[field] for field in fieldnames))
    missing_rows_count, missing_rows_percentage = format_count_and_percentage(missing_rows, total_students)

    print("### Dataset Summary")
    print()
    print(f"- Total students: {total_students}")

    print(f"- Students with any missing values: {missing_rows_count} ({missing_rows_percentage})")
    print(f"- Suppression threshold: {SUPPRESS_THRESHOLD} students")
    print()

    print()
    print("### Missing Data")
    print()
    print("| Field | Missing values | % of students |")
    print("| --- | --- | --- |")
    for field, missing, percentage in summarise_missing_data(rows, fieldnames):
        print(f"| {field} | {missing} | {percentage} |")
    print()

    print("### Student Counts by School")
    print()
    print("| School | Students |")
    print("| --- | --- |")
    school_counter = Counter(row["school_id"] for row in rows)
    for school, count in summarise_counter(school_counter):
        print(f"| {school} | {count} |")
    print()

    print("### Student Counts by Sex")
    print()
    print("| Sex | Students |")
    print("| --- | --- |")
    sex_counter = Counter(row["sex"] for row in rows)
    for sex, count in summarise_counter(sex_counter):
        print(f"| {sex} | {count} |")
    print()

    print("### Support And Funding Indicators")
    print()
    print("| Field | Yes | No | Other |")
    print("| --- | --- | --- | --- |")
    for field in ("pp", "eal", "send", "ehcp", "lac"):
        yes, no, other = summarise_boolean_field(rows, field)
        print(f"| {field} | {yes} | {no} | {other} |")
    print()

    print("### Key Stage 2 Score Summary")
    print()
    for field in ["ks2_maths_score", "ks2_reading_score"]:
        print(f"#### {field}")
        for label, value in summarise_scores(rows, field):
            print(f"- {label}: {value}")
        print()


def write_teacher_section(input_dir):
    rows, fieldnames = load_csv(input_dir / "teachers.csv")

    print("## Teachers")
    print()

    total_teachers = len(rows)
    missing_rows = sum(1 for row in rows if any(not row[field] for field in fieldnames))
    missing_rows_count, missing_rows_percentage = format_count_and_percentage(missing_rows, total_teachers)

    print("### Dataset Summary")
    print()
    print(f"- Total teachers: {total_teachers}")

    print(f"- Teachers with any missing values: {missing_rows_count} ({missing_rows_percentage})")
    print(f"- Suppression threshold: {SUPPRESS_THRESHOLD} teachers")
    print()

    print()
    print("### Missing Data")
    print()
    print("| Field | Missing values | % of teachers |")
    print("| --- | --- | --- |")
    for field, missing, percentage in summarise_missing_data(rows, fieldnames):
        print(f"| {field} | {missing} | {percentage} |")
    print()

    print("### Teacher Counts by Payscale")
    print()
    print("| Payscale | Teachers |")
    print("| --- | --- |")
    payscale_counter = Counter(row["payscale"] for row in rows)
    for payscale, count in summarise_counter(payscale_counter):
        print(f"| {payscale} | {count} |")
    print()


def write_results_section(input_dir):
    rows, fieldnames = load_csv(input_dir / "results.csv")

    print("## Results")
    print()

    total_records = len(rows)

    student_ids = {row["student_id"] for row in rows}
    teacher_ids = {row["teacher_id"] for row in rows if "teacher_id" in row}
    class_ids = {row["class_id"] for row in rows if "class_id" in row}

    date_values = [row["date"] for row in rows]
    date_values.sort()
    earliest_date = date_values[0] if date_values else None
    latest_date = date_values[-1] if date_values else None

    missing_rows = sum(1 for row in rows if any(not row[field] for field in fieldnames))
    missing_count_display, missing_percentage_display = format_count_and_percentage(missing_rows, total_records)

    print("### Dataset Summary")
    print()
    print(f"- Total records: {total_records}")
    print(f"- Students represented: {safe_count(len(student_ids))}")
    print(f"- Teachers represented: {safe_count(len(teacher_ids))}")
    print(f"- Classes represented: {safe_count(len(class_ids))}")
    print(f"- Earliest assessment date: {earliest_date}")
    print(f"- Latest assessment date: {latest_date}")
    print(f"- Records with any missing values: {missing_count_display} ({missing_percentage_display})")
    print(f"- Suppression threshold: {SUPPRESS_THRESHOLD} records")
    print()

    print("### Missing Data")
    print()
    print("| Field | Missing values | % of records |")
    print("| --- | --- | --- |")
    for field, missing, percentage in summarise_missing_data(rows, fieldnames):
        print(f"| {field} | {missing} | {percentage} |")
    print()

    print("### Academic Years")
    print()
    print("| academic_year | Records |")
    print("| --- | --- |")
    academic_year_counter = Counter(row["academic_year"] for row in rows)
    for academic_year, count in summarise_counter(academic_year_counter):
        print(f"| {academic_year} | {count} |")

    print("### Year Groups")
    print()
    print("| year_group | Records |")
    print("| --- | --- |")
    year_group_counter = Counter(row["year_group"] for row in rows)
    for year_group, count in sorted(year_group_counter.items(), key=lambda item: year_group_sort_key(item[0])):
        print(f"| {year_group} | {safe_count(count)} |")
    print()

    write_scores_summary(rows)


def summarise_missing_data(rows, fieldnames):
    total_rows = len(rows)
    summaries = []
    for field in fieldnames:
        missing = sum(1 for row in rows if not row[field])
        count_display, percentage_display = format_count_and_percentage(missing, total_rows)
        summaries.append((field, count_display, percentage_display))
    return summaries


def summarise_scores(rows, field):
    scores = parse_scores(rows, field)
    if not scores:
        return [("Count", "0")]

    sorted_scores = sorted(scores)
    metrics = []

    metrics.append(("Count", str(len(scores))))
    metrics.append(("Mean", format_float(mean(scores))))
    metrics.append(("Median", format_float(median(scores))))

    if len(scores) >= 2:
        metrics.append(("Standard deviation", format_float(stdev(scores))))
    else:
        metrics.append(("Standard deviation", "N/A"))

    for label, fraction in (("25th percentile", 0.25), ("75th percentile", 0.75)):
        metrics.append((label, format_float(percentile(sorted_scores, fraction))))

    return metrics


def format_float(value):
    return f"{value:.1f}"


def percentile(sorted_values, fraction):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    index = (len(sorted_values) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return float(sorted_values[int(index)])

    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    weight = index - lower
    return lower_value * (1 - weight) + upper_value * weight


def summarise_boolean_field(rows, field):
    truthy = sum(1 for row in rows if row[field].upper() == "T")
    falsy = sum(1 for row in rows if row[field].upper() == "F")
    other = len(rows) - truthy - falsy

    yes_display = safe_count(truthy)
    no_display = safe_count(falsy)
    if other:
        return yes_display, no_display, safe_count(other)
    return yes_display, no_display, "0"


def write_scores_summary(rows):
    print("### Scores")
    print()

    print("| Assessment type | Score type | Records | No class | No teacher | Year groups | Subjects |")
    print("| --- | --- | --- | --- | --- | --- | --- |")

    grouped_rows = defaultdict(list)
    for row in rows:
        grouped_rows[row["assessment_type"]].append(row)

    for assessment_type in sorted(grouped_rows):
        rows = grouped_rows[assessment_type]
        score_type = classify_score_type(rows)
        record_count = safe_count(len(rows))
        no_class = sum(1 for row in rows if not row["class_id"])
        no_teacher = sum(1 for row in rows if not row["teacher_id"])
        year_groups = summarise_year_groups(rows)
        subjects = summarise_subjects(rows)
        print(
            f"| {assessment_type} | {score_type} | {record_count} | {safe_count(no_class)} | {safe_count(no_teacher)} | {year_groups} | {subjects} |"
        )
    print()


def classify_score_type(rows):
    scores = {row["score"] for row in rows}

    if len(scores) == 1:
        return f"All scores the same: {list(scores)[0]}"

    if all(re.fullmatch(r"\d[ABCDE]\+?", score) for score in scores):
        return f"Eg 8A+ (min: {min(scores)}, max: {max(scores)})"
    if scores <= LETTER_GRADES:
        return "Letter grades"
    if scores <= KS5_NVQ_GRADES:
        return "KS5 NVQ grades"
    if scores <= (LETTER_GRADES | KS5_NVQ_GRADES):
        return "Letter grades with KS5 NVQ grades"
    if scores <= NUMBER_GRADES:
        return f"Numeric grades (min: {min(scores)}, max: {max(scores)})"
    if scores <= KS4_NVQ_GRADES:
        return "KS4 NVQ grades"
    if scores <= (NUMBER_GRADES | KS4_NVQ_GRADES):
        return "Numeric grades with KS4 NVQ grades"
    if scores <= PLUS_MINUS_SCORES:
        return "One of: --, -, =, +, ++"
    if scores == {"F", "H"}:
        return "One of: F or H (Foundation or Higher)"

    try:
        numeric_scores = [float(score) for score in scores]
        return f"Numeric (min: {min(numeric_scores)}, max: {max(numeric_scores)})"
    except ValueError:
        return "Unknown grading system"


def summarise_year_groups(rows):
    counter = Counter(row["year_group"] for row in rows)
    items = []
    for year_group, count in sorted(counter.items(), key=lambda item: (year_group_sort_key(item[0]), item[0])):
        items.append(f"{year_group} ({format_detail_count(count)})")
    return "; ".join(items) if items else "-"


def year_group_sort_key(year_group):
    if year_group == "R":
        return 0
    match = re.search(r"\d+", year_group)
    return int(match.group())


def summarise_subjects(rows):
    counter = Counter(row["subject"] for row in rows)
    items = []
    for subject, count in sorted(counter.items()):
        items.append(f"{subject} ({format_detail_count(count)})")
    return "; ".join(items) if items else "-"


def safe_count(count):
    if count == 0:
        return "0"
    elif count < SUPPRESS_THRESHOLD:
        return f"<{SUPPRESS_THRESHOLD} (suppressed)"
    return str(count)


def format_percentage(count, total):
    if total == 0:
        return "N/A"
    if count == 0:
        return "0.0%"
    if 0 < count < SUPPRESS_THRESHOLD:
        upper_count = min(SUPPRESS_THRESHOLD - 1, total)
        if upper_count <= 0:
            return "Suppressed"
        upper_percent = upper_count / total * 100
        if upper_percent >= 100:
            return "<=100.0% (suppressed)"
        return f"<={upper_percent:.1f}% (suppressed)"
    return f"{(count / total) * 100:.1f}%"


def format_count_and_percentage(count, total):
    return safe_count(count), format_percentage(count, total)


def summarise_counter(counter):
    return [(key, safe_count(count)) for key, count in sorted(counter.items())]


def parse_scores(rows, field):
    return [float(row[field]) for row in rows if row[field]]


def format_detail_count(count):
    if count > SUPPRESS_THRESHOLD:
        return str(count)
    return "-"


def load_csv(path):
    with path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    return rows, reader.fieldnames


if __name__ == "__main__":
    main()
