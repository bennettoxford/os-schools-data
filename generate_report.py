import argparse
import csv
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from statistics import mean, median, stdev

SUPPRESS_THRESHOLD = 7
ROUND_BASE = 5
SCHOOL_FIELD = "school_id"

JUNIOR_GRADES = {"EM", "WBS", "WBS1", "WBS2", "WBS3", "WBS4", "WBS5", "WBS6", "WTS", "WTS+", "EXS", "GDS"}
LETTER_GRADES = set("ABCDEU") | {"A*"}
NUMBER_GRADES = {"1", "11", "2", "22", "3", "33", "4", "44", "5", "55", "6", "66", "7", "77", "8", "88", "9", "99", "U"}
KS4_NVQ_GRADES = {"D1*", "D1", "D2", "M1", "M2", "P", "P1", "P2", "U"}
KS5_NVQ_GRADES = {"D*", "D*D*D*", "D*DD", "D*D*D", "D*DD", "DDD", "DDM", "DMM", "DMP", "DPP", "MMM", "MPP", "MMP", "PPP", "U"}
BTEC_GRADES = set("DMP")
PLUS_MINUS_SCORES = {"--", "-", "=", "+", "++"}
WORSE_SAME_BETTER = {"Worse", "Same", "Better"}
OUTPUT_STREAM = sys.stdout


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Generate a markdown data report from CSV files.")
    parser.add_argument("input_dir", type=Path, help="Directory containing students.csv, teachers.csv, and results.csv.")
    parser.add_argument("title", help="Report title.")
    parser.add_argument(
        "--output",
        type=argparse.FileType("w", encoding="utf-8"),
        default=sys.stdout,
        help="Write report to file instead of stdout.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    global OUTPUT_STREAM
    args = parse_args(argv)

    input_dir = args.input_dir
    title = args.title
    OUTPUT_STREAM = args.output

    try:
        run_report(input_dir, title)
    finally:
        if args.output is not sys.stdout:
            args.output.close()


def emit(*args, **kwargs):
    kwargs.setdefault("file", OUTPUT_STREAM)
    print(*args, **kwargs)


def run_report(input_dir, title):

    emit(f"# TED Data Report: {title}")
    emit()
    emit(f"Generated on {date.today()}.")
    emit()

    students_rows, students_fieldnames = load_csv(input_dir / "students.csv")
    teachers_rows, teachers_fieldnames = load_csv(input_dir / "teachers.csv")
    results_rows, results_fieldnames = load_csv(input_dir / "results.csv")

    write_student_section(students_rows, students_fieldnames, base_level=2, include_school_counts=True)
    emit()
    write_teacher_section(teachers_rows, teachers_fieldnames, base_level=2)
    emit()
    write_results_section(results_rows, results_fieldnames, base_level=2)

    student_school_field = SCHOOL_FIELD
    teacher_school_field = SCHOOL_FIELD
    results_school_field = SCHOOL_FIELD
    if student_school_field not in students_fieldnames:
        raise ValueError("students.csv must include a school_id column for per-school reporting.")
    if teacher_school_field not in teachers_fieldnames:
        raise ValueError("teachers.csv must include a school_id column for per-school reporting.")
    if results_school_field not in results_fieldnames:
        raise ValueError("results.csv must include a school_id column for per-school reporting.")

    schools = []
    if student_school_field:
        schools = sorted({row[student_school_field] for row in students_rows if row.get(student_school_field)})

    if not schools:
        return

    emit()

    for school in schools:
        emit()
        heading(2, f"School: {school}")
        emit()
        if student_school_field:
            school_students = [row for row in students_rows if row.get(student_school_field) == school]
            write_student_section(school_students, students_fieldnames, base_level=3, include_school_counts=False)
            emit()
        school_teachers = [row for row in teachers_rows if row.get(teacher_school_field) == school]
        write_teacher_section(school_teachers, teachers_fieldnames, base_level=3)
        emit()
        school_results = [row for row in results_rows if row.get(results_school_field) == school]
        write_results_section(school_results, results_fieldnames, base_level=3)


def heading(level, text):
    emit(f"{'#' * level} {text}")


def write_student_section(rows, fieldnames, base_level=2, include_school_counts=True):
    heading(base_level, "Students")
    emit()

    total_students = len(rows)
    missing_rows = sum(1 for row in rows if any(not row[field] for field in fieldnames))
    missing_rows_count, missing_rows_percentage = format_count_and_percentage(missing_rows, total_students)

    heading(base_level + 1, "Dataset Summary")
    emit()
    emit(f"- Total students: {safe_count(total_students)}")

    emit(f"- Students with any missing values: {missing_rows_count} ({missing_rows_percentage})")
    emit(f"- Suppression threshold: {SUPPRESS_THRESHOLD} students")
    emit(f"- Counts rounded to nearest {ROUND_BASE}")
    emit()

    emit()
    heading(base_level + 1, "Missing Data")
    emit()
    emit("| Field | Missing values (rounded) | % of students |")
    emit("| --- | --- | --- |")
    for field, missing, percentage in summarise_missing_data(rows, fieldnames):
        emit(f"| {field} | {missing} | {percentage} |")
    emit()

    if include_school_counts:
        heading(base_level + 1, "Student Counts by School")
        emit()
        if SCHOOL_FIELD not in fieldnames:
            raise ValueError("students.csv must include a school_id column for per-school reporting.")
        emit("| School | Students (rounded) |")
        emit("| --- | --- |")
        school_counter = Counter(row[SCHOOL_FIELD] for row in rows)
        for school, count in summarise_counter(school_counter):
            emit(f"| {school} | {count} |")
        emit()

    heading(base_level + 1, "Student Counts by Sex")
    emit()
    emit("| Sex | Students (rounded) |")
    emit("| --- | --- |")
    sex_counter = Counter(row["sex"] for row in rows)
    for sex, count in summarise_counter(sex_counter):
        emit(f"| {sex} | {count} |")
    emit()

    heading(base_level + 1, "Support And Funding Indicators")
    emit()
    emit("| Field | Yes (rounded) | No (rounded) | Other (rounded) |")
    emit("| --- | --- | --- | --- |")
    for field in ("pp", "eal", "send", "ehcp", "lac"):
        yes, no, other = summarise_boolean_field(rows, field)
        emit(f"| {field} | {yes} | {no} | {other} |")
    emit()

    heading(base_level + 1, "Key Stage 2 Score Summary")
    emit()
    for field in ["ks2_maths_score", "ks2_reading_score"]:
        heading(base_level + 2, field)
        for label, value in summarise_scores(rows, field):
            emit(f"- {label}: {value}")
        emit()


def write_teacher_section(rows, fieldnames, base_level=2):
    heading(base_level, "Teachers")
    emit()

    total_teachers = len(rows)
    missing_rows = sum(1 for row in rows if any(not row[field] for field in fieldnames))
    missing_rows_count, missing_rows_percentage = format_count_and_percentage(missing_rows, total_teachers)

    heading(base_level + 1, "Dataset Summary")
    emit()
    emit(f"- Total teachers: {safe_count(total_teachers)}")

    emit(f"- Teachers with any missing values: {missing_rows_count} ({missing_rows_percentage})")
    emit(f"- Suppression threshold: {SUPPRESS_THRESHOLD} teachers")
    emit(f"- Counts rounded to nearest {ROUND_BASE}")
    emit()

    emit()
    heading(base_level + 1, "Missing Data")
    emit()
    emit("| Field | Missing values (rounded) | % of teachers |")
    emit("| --- | --- | --- |")
    for field, missing, percentage in summarise_missing_data(rows, fieldnames):
        emit(f"| {field} | {missing} | {percentage} |")
    emit()

    heading(base_level + 1, "Teacher Counts by Payscale")
    emit()
    emit("| Payscale | Teachers (rounded) |")
    emit("| --- | --- |")
    payscale_counter = Counter(row["payscale"] for row in rows)
    for payscale, count in summarise_counter(payscale_counter):
        emit(f"| {payscale} | {count} |")
    emit()


def write_results_section(rows, fieldnames, base_level=2):
    heading(base_level, "Results")
    emit()

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

    heading(base_level + 1, "Dataset Summary")
    emit()
    emit(f"- Total records: {safe_count(total_records)}")
    emit(f"- Students represented: {safe_count(len(student_ids))}")
    emit(f"- Teachers represented: {safe_count(len(teacher_ids))}")
    emit(f"- Classes represented: {safe_count(len(class_ids))}")
    emit(f"- Earliest assessment date: {earliest_date}")
    emit(f"- Latest assessment date: {latest_date}")
    emit(f"- Records with any missing values: {missing_count_display} ({missing_percentage_display})")
    emit(f"- Suppression threshold: {SUPPRESS_THRESHOLD} records")
    emit(f"- Counts rounded to nearest {ROUND_BASE}")
    emit()

    heading(base_level + 1, "Missing Data")
    emit()
    emit("| Field | Missing values (rounded) | % of records |")
    emit("| --- | --- | --- |")
    for field, missing, percentage in summarise_missing_data(rows, fieldnames):
        emit(f"| {field} | {missing} | {percentage} |")
    emit()

    heading(base_level + 1, "Academic Years")
    emit()
    emit("| academic_year | Records (rounded) |")
    emit("| --- | --- |")
    academic_year_counter = Counter(row["academic_year"] for row in rows)
    for academic_year, count in summarise_counter(academic_year_counter):
        emit(f"| {academic_year} | {count} |")

    heading(base_level + 1, "Year Groups")
    emit()
    emit("| year_group | Records (rounded) |")
    emit("| --- | --- |")
    year_group_counter = Counter(row["year_group"] for row in rows)
    for year_group, count in sorted(year_group_counter.items(), key=lambda item: year_group_sort_key(item[0])):
        emit(f"| {year_group} | {safe_count(count)} |")
    emit()

    write_scores_summary(rows, base_level=base_level + 1)


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

    metrics.append(("Count", safe_count(len(scores))))
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


def write_scores_summary(rows, base_level=3):
    heading(base_level, "Scores")
    emit()

    emit("| Assessment type | Score type | Records (rounded) | No class (rounded) | No teacher (rounded) | Year groups (rounded) | Subjects (rounded) |")
    emit("| --- | --- | --- | --- | --- | --- | --- |")

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
        emit(
            f"| {assessment_type} | {score_type} | {record_count} | {safe_count(no_class)} | {safe_count(no_teacher)} | {year_groups} | {subjects} |"
        )
    emit()


def classify_score_type(rows):
    scores = {row["score"] for row in rows}

    if len(scores) == 1:
        return f"All scores the same: {list(scores)[0]}"

    if all(re.fullmatch(r"\d[ABCDE]\+?", score) for score in scores):
        return f"Eg 8A+ (min: {min(scores)}, max: {max(scores)})"
    if all(re.fullmatch(r"\d+:\d\d", score) for score in scores):
        return "Reading ages (eg 12:04)"
    if scores <= JUNIOR_GRADES:
        return "Junior grades"
    if scores <= LETTER_GRADES:
        return "Letter grades"
    if scores <= BTEC_GRADES:
        return "BTEC grades"
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
    if scores <= WORSE_SAME_BETTER:
        return "One of: Worse, Same, Better"
    if scores <= {"EXP-", "EXP", "EXP+"}:
        return "One of: EXP-, EXP, EXP+"
    if scores == {"Y", "N"}:
        return "One of: Y or No"
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
    return str(round_count(count))


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
        return str(round_count(count))
    return "-"


def round_count(count):
    return int(ROUND_BASE * round(count / ROUND_BASE))


def load_csv(path):
    with path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    return rows, reader.fieldnames


if __name__ == "__main__":
    main()
