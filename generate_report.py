import argparse
import csv
import math
import re
import sys
from array import array
from collections import Counter, defaultdict
from dataclasses import dataclass, field
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


@dataclass
class StudentSummary:
    total_rows: int = 0
    missing_rows: int = 0
    missing_by_field: dict[str, int] = field(default_factory=dict)
    school_counter: Counter = field(default_factory=Counter)
    sex_counter: Counter = field(default_factory=Counter)
    boolean_counters: dict[str, Counter] = field(default_factory=dict)
    score_values: dict[str, array] = field(default_factory=dict)


@dataclass
class TeacherSummary:
    total_rows: int = 0
    missing_rows: int = 0
    missing_by_field: dict[str, int] = field(default_factory=dict)
    payscale_counter: Counter = field(default_factory=Counter)


@dataclass
class ScoreGroupSummary:
    record_count: int = 0
    no_class: int = 0
    no_teacher: int = 0
    year_group_counter: Counter = field(default_factory=Counter)
    subject_counter: Counter = field(default_factory=Counter)
    scores: set[str] = field(default_factory=set)


@dataclass
class ResultsSummary:
    total_rows: int = 0
    missing_rows: int = 0
    missing_by_field: dict[str, int] = field(default_factory=dict)
    student_ids: set[str] = field(default_factory=set)
    teacher_ids: set[str] = field(default_factory=set)
    class_ids: set[str] = field(default_factory=set)
    earliest_date: str | None = None
    latest_date: str | None = None
    academic_year_counter: Counter = field(default_factory=Counter)
    year_group_counter: Counter = field(default_factory=Counter)
    assessment_groups: dict[str, ScoreGroupSummary] = field(default_factory=dict)


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
    students_path = input_dir / "students.csv"
    teachers_path = input_dir / "teachers.csv"
    results_path = input_dir / "results.csv"

    student_fieldnames = get_csv_fieldnames(students_path)
    teacher_fieldnames = get_csv_fieldnames(teachers_path)
    results_fieldnames = get_csv_fieldnames(results_path)

    if SCHOOL_FIELD not in student_fieldnames:
        raise ValueError("students.csv must include a school_id column for per-school reporting.")
    if SCHOOL_FIELD not in teacher_fieldnames:
        raise ValueError("teachers.csv must include a school_id column for per-school reporting.")
    if SCHOOL_FIELD not in results_fieldnames:
        raise ValueError("results.csv must include a school_id column for per-school reporting.")

    student_summary, student_school_summaries = build_student_summaries(students_path, student_fieldnames)
    teacher_summary, teacher_school_summaries = build_teacher_summaries(teachers_path, teacher_fieldnames)
    results_summary, results_school_summaries = build_results_summaries(results_path, results_fieldnames)

    emit(f"# TED Data Report: {title}")
    emit()
    emit(f"Generated on {date.today()}.")
    emit()

    write_student_section(student_summary, student_fieldnames, base_level=2, include_school_counts=True)
    emit()
    write_teacher_section(teacher_summary, teacher_fieldnames, base_level=2)
    emit()
    write_results_section(results_summary, results_fieldnames, base_level=2)

    schools = sorted(school for school in student_school_summaries if school)
    if not schools:
        return

    emit()

    for school in schools:
        emit()
        heading(2, f"School: {school}")
        emit()
        write_student_section(student_school_summaries[school], student_fieldnames, base_level=3, include_school_counts=False)
        emit()
        write_teacher_section(teacher_school_summaries.get(school, new_teacher_summary(teacher_fieldnames)), teacher_fieldnames, base_level=3)
        emit()
        write_results_section(results_school_summaries.get(school, new_results_summary(results_fieldnames)), results_fieldnames, base_level=3)


def heading(level, text):
    emit(f"{'#' * level} {text}")


def get_csv_fieldnames(path):
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or [])


def iter_csv_rows(path, fieldnames):
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, fieldnames=fieldnames)
        next(reader, None)
        yield from reader


def new_student_summary(fieldnames):
    return StudentSummary(
        missing_by_field={field: 0 for field in fieldnames},
        boolean_counters={field: Counter() for field in ("pp", "eal", "send", "ehcp", "lac")},
        score_values={field: array("d") for field in ("ks2_maths_score", "ks2_reading_score")},
    )


def build_student_summaries(path, fieldnames):
    overall = new_student_summary(fieldnames)
    by_school = {}
    for row in iter_csv_rows(path, fieldnames):
        update_student_summary(overall, row, fieldnames)
        school = intern_if_present(row.get(SCHOOL_FIELD, ""))
        if school:
            school_summary = by_school.get(school)
            if school_summary is None:
                school_summary = new_student_summary(fieldnames)
                by_school[school] = school_summary
            update_student_summary(school_summary, row, fieldnames)
    return overall, by_school


def update_student_summary(summary, row, fieldnames):
    summary.total_rows += 1
    row_has_missing = False
    for field in fieldnames:
        value = row.get(field, "")
        if not value:
            summary.missing_by_field[field] += 1
            row_has_missing = True
    if row_has_missing:
        summary.missing_rows += 1

    school = intern_if_present(row.get(SCHOOL_FIELD, ""))
    if school:
        summary.school_counter[school] += 1

    summary.sex_counter[intern_if_present(row.get("sex", ""))] += 1

    for field in ("pp", "eal", "send", "ehcp", "lac"):
        summary.boolean_counters[field][intern_if_present(normalise_string(row.get(field, "")))] += 1

    for field in ("ks2_maths_score", "ks2_reading_score"):
        value = row.get(field, "")
        if value:
            summary.score_values[field].append(float(value))


def write_student_section(summary, fieldnames, base_level=2, include_school_counts=True):
    heading(base_level, "Students")
    emit()

    total_students = summary.total_rows
    missing_rows_count, missing_rows_percentage = format_count_and_percentage(summary.missing_rows, total_students)

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
    for field, missing, percentage in summarise_missing_data(summary.missing_by_field, fieldnames, total_students):
        emit(f"| {field} | {missing} | {percentage} |")
    emit()

    if include_school_counts:
        heading(base_level + 1, "Student Counts by School")
        emit()
        emit("| School | Students (rounded) |")
        emit("| --- | --- |")
        for school, count in summarise_counter(summary.school_counter):
            emit(f"| {school} | {count} |")
        emit()

    heading(base_level + 1, "Student Counts by Sex")
    emit()
    emit("| Sex | Students (rounded) |")
    emit("| --- | --- |")
    for sex, count in summarise_counter(summary.sex_counter):
        emit(f"| {sex} | {count} |")
    emit()

    heading(base_level + 1, "Support And Funding Indicators")
    emit()
    emit("| Field | Yes (rounded) | No (rounded) | Other (rounded) |")
    emit("| --- | --- | --- | --- |")
    for field in ("pp", "eal", "send", "ehcp", "lac"):
        yes, no, other = summarise_boolean_counter(summary.boolean_counters[field], total_students)
        emit(f"| {field} | {yes} | {no} | {other} |")
    emit()

    heading(base_level + 1, "Key Stage 2 Score Summary")
    emit()
    for field in ("ks2_maths_score", "ks2_reading_score"):
        heading(base_level + 2, field)
        for label, value in summarise_scores(summary.score_values[field]):
            emit(f"- {label}: {value}")
        emit()


def new_teacher_summary(fieldnames):
    return TeacherSummary(missing_by_field={field: 0 for field in fieldnames})


def build_teacher_summaries(path, fieldnames):
    overall = new_teacher_summary(fieldnames)
    by_school = {}
    for row in iter_csv_rows(path, fieldnames):
        update_teacher_summary(overall, row, fieldnames)
        school = intern_if_present(row.get(SCHOOL_FIELD, ""))
        if school:
            school_summary = by_school.get(school)
            if school_summary is None:
                school_summary = new_teacher_summary(fieldnames)
                by_school[school] = school_summary
            update_teacher_summary(school_summary, row, fieldnames)
    return overall, by_school


def update_teacher_summary(summary, row, fieldnames):
    summary.total_rows += 1
    row_has_missing = False
    for field in fieldnames:
        value = row.get(field, "")
        if not value:
            summary.missing_by_field[field] += 1
            row_has_missing = True
    if row_has_missing:
        summary.missing_rows += 1
    summary.payscale_counter[intern_if_present(row.get("payscale", ""))] += 1


def write_teacher_section(summary, fieldnames, base_level=2):
    heading(base_level, "Teachers")
    emit()

    total_teachers = summary.total_rows
    missing_rows_count, missing_rows_percentage = format_count_and_percentage(summary.missing_rows, total_teachers)

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
    for field, missing, percentage in summarise_missing_data(summary.missing_by_field, fieldnames, total_teachers):
        emit(f"| {field} | {missing} | {percentage} |")
    emit()

    heading(base_level + 1, "Teacher Counts by Payscale")
    emit()
    emit("| Payscale | Teachers (rounded) |")
    emit("| --- | --- |")
    for payscale, count in summarise_counter(summary.payscale_counter):
        emit(f"| {payscale} | {count} |")
    emit()


def new_score_group_summary():
    return ScoreGroupSummary()


def new_results_summary(fieldnames):
    return ResultsSummary(
        missing_by_field={field: 0 for field in fieldnames},
        assessment_groups=defaultdict(new_score_group_summary),
    )


def build_results_summaries(path, fieldnames):
    overall = new_results_summary(fieldnames)
    by_school = {}
    for row in iter_csv_rows(path, fieldnames):
        update_results_summary(overall, row, fieldnames)
        school = intern_if_present(row.get(SCHOOL_FIELD, ""))
        if school:
            school_summary = by_school.get(school)
            if school_summary is None:
                school_summary = new_results_summary(fieldnames)
                by_school[school] = school_summary
            update_results_summary(school_summary, row, fieldnames)
    return overall, by_school


def update_results_summary(summary, row, fieldnames):
    summary.total_rows += 1

    row_has_missing = False
    for field in fieldnames:
        value = row.get(field, "")
        if not value:
            summary.missing_by_field[field] += 1
            row_has_missing = True
    if row_has_missing:
        summary.missing_rows += 1

    student_id = intern_if_present(row.get("student_id", ""))
    teacher_id = intern_if_present(row.get("teacher_id", ""))
    class_id = intern_if_present(row.get("class_id", ""))
    date_value = row.get("date", "")
    academic_year = intern_if_present(row.get("academic_year", ""))
    year_group = intern_if_present(row.get("year_group", ""))
    assessment_type = intern_if_present(row.get("assessment_type", ""))
    subject = intern_if_present(row.get("subject", ""))
    score = intern_if_present(row.get("score", ""))

    if student_id:
        summary.student_ids.add(student_id)
    if teacher_id:
        summary.teacher_ids.add(teacher_id)
    if class_id:
        summary.class_ids.add(class_id)

    if date_value:
        if summary.earliest_date is None or date_value < summary.earliest_date:
            summary.earliest_date = date_value
        if summary.latest_date is None or date_value > summary.latest_date:
            summary.latest_date = date_value

    summary.academic_year_counter[academic_year] += 1
    summary.year_group_counter[year_group] += 1

    group = summary.assessment_groups[assessment_type]
    group.record_count += 1
    if not class_id:
        group.no_class += 1
    if not teacher_id:
        group.no_teacher += 1
    group.year_group_counter[year_group] += 1
    group.subject_counter[subject] += 1
    if score:
        group.scores.add(score)


def write_results_section(summary, fieldnames, base_level=2):
    heading(base_level, "Results")
    emit()

    total_records = summary.total_rows
    missing_count_display, missing_percentage_display = format_count_and_percentage(summary.missing_rows, total_records)

    heading(base_level + 1, "Dataset Summary")
    emit()
    emit(f"- Total records: {safe_count(total_records)}")
    emit(f"- Students represented: {safe_count(len(summary.student_ids))}")
    emit(f"- Teachers represented: {safe_count(len(summary.teacher_ids))}")
    emit(f"- Classes represented: {safe_count(len(summary.class_ids))}")
    emit(f"- Earliest assessment date: {summary.earliest_date}")
    emit(f"- Latest assessment date: {summary.latest_date}")
    emit(f"- Records with any missing values: {missing_count_display} ({missing_percentage_display})")
    emit(f"- Suppression threshold: {SUPPRESS_THRESHOLD} records")
    emit(f"- Counts rounded to nearest {ROUND_BASE}")
    emit()

    heading(base_level + 1, "Missing Data")
    emit()
    emit("| Field | Missing values (rounded) | % of records |")
    emit("| --- | --- | --- |")
    for field, missing, percentage in summarise_missing_data(summary.missing_by_field, fieldnames, total_records):
        emit(f"| {field} | {missing} | {percentage} |")
    emit()

    heading(base_level + 1, "Academic Years")
    emit()
    emit("| academic_year | Records (rounded) |")
    emit("| --- | --- |")
    for academic_year, count in summarise_counter(summary.academic_year_counter):
        emit(f"| {academic_year} | {count} |")

    heading(base_level + 1, "Year Groups")
    emit()
    emit("| year_group | Records (rounded) |")
    emit("| --- | --- |")
    for year_group, count in sorted(summary.year_group_counter.items(), key=lambda item: year_group_sort_key(item[0])):
        emit(f"| {year_group} | {safe_count(count)} |")
    emit()

    write_scores_summary(summary.assessment_groups, base_level=base_level + 1)


def summarise_missing_data(missing_by_field, fieldnames, total_rows):
    summaries = []
    for field in fieldnames:
        missing = missing_by_field[field]
        count_display, percentage_display = format_count_and_percentage(missing, total_rows)
        summaries.append((field, count_display, percentage_display))
    return summaries


def summarise_scores(scores):
    if not scores:
        return [("Count", "0")]

    sorted_scores = sorted(scores)
    metrics = []

    metrics.append(("Count", safe_count(len(scores))))
    metrics.append(("Mean", format_float(mean(scores))))
    metrics.append(("Median", format_float(median(sorted_scores))))

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


def summarise_boolean_counter(counter, total_rows):
    truthy = counter.get("T", 0)
    falsy = counter.get("F", 0)
    other = total_rows - truthy - falsy

    yes_display = safe_count(truthy)
    no_display = safe_count(falsy)
    if other:
        return yes_display, no_display, safe_count(other)
    return yes_display, no_display, "0"


def write_scores_summary(assessment_groups, base_level=3):
    heading(base_level, "Scores")
    emit()

    emit("| Assessment type | Score type | Records (rounded) | No class (rounded) | No teacher (rounded) | Year groups (rounded) | Subjects (rounded) |")
    emit("| --- | --- | --- | --- | --- | --- | --- |")

    for assessment_type in sorted(assessment_groups):
        group = assessment_groups[assessment_type]
        score_type = classify_score_type(group.scores)
        record_count = safe_count(group.record_count)
        no_class = safe_count(group.no_class)
        no_teacher = safe_count(group.no_teacher)
        year_groups = summarise_year_groups(group.year_group_counter)
        subjects = summarise_subjects(group.subject_counter)
        emit(f"| {assessment_type} | {score_type} | {record_count} | {no_class} | {no_teacher} | {year_groups} | {subjects} |")
    emit()


def classify_score_type(scores):
    if not scores:
        return "Unknown grading system"

    if len(scores) == 1:
        return f"All scores the same: {next(iter(scores))}"

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


def summarise_year_groups(counter):
    items = []
    for year_group, count in sorted(counter.items(), key=lambda item: (year_group_sort_key(item[0]), item[0])):
        items.append(f"{year_group} ({format_detail_count(count)})")
    return "; ".join(items) if items else "-"


def year_group_sort_key(year_group):
    if year_group == "R":
        return 0
    match = re.search(r"\d+", year_group)
    return int(match.group()) if match else float("inf")


def summarise_subjects(counter):
    items = []
    for subject, count in sorted(counter.items()):
        items.append(f"{subject} ({format_detail_count(count)})")
    return "; ".join(items) if items else "-"


def normalise_string(value):
    return value.strip().upper()


def intern_if_present(value):
    if value:
        return sys.intern(value)
    return ""


def safe_count(count):
    if count == 0:
        return "0"
    if count < SUPPRESS_THRESHOLD:
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


def format_detail_count(count):
    if count > SUPPRESS_THRESHOLD:
        return str(round_count(count))
    return "-"


def round_count(count):
    return int(ROUND_BASE * round(count / ROUND_BASE))


if __name__ == "__main__":
    main()
