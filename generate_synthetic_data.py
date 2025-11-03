# See README for more details.

import csv
import random
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path


# This ensures that the same data is generated each time the script is run.
random.seed(12345)

# Various parameters indicating the number of schools/teachers/students and the available subjects.
num_schools = 1
num_teachers_per_school = 20
num_students_per_year = 150
nvq_subjects = ["Construction", "Hospitality"]
non_nvq_subjects = ["English", "Mathematics", "Science"]
subjects = nvq_subjects + non_nvq_subjects

# The probabilities that a given student has given attributes.
prob_pp = 0.15
prob_eal = 0.20
prob_send = 0.10
prob_ehcp_given_send = 0.25
prob_lac = 0.01

# At the moment, the TED database contains data from one academic year.
academic_year = "2024/2025"


def main(output_dir):
    results = []
    students = []

    for school_ix in range(num_schools):
        generate_school_data(school_ix, results, students)

    write_output(output_dir, results, students)


def generate_school_data(school_ix, results, students):
    school_id = f"SCH{school_ix:02}"
    school_effect = random.gauss(mu=0, sigma=10)

    classes = build_classes(school_ix)

    for student_id in range(num_students_per_year * 7):
        # Generate student attributes.
        year_group = random.choice(range(7, 14))
        sex = random.choice(["M", "F"])
        pp = random.random() < prob_pp
        eal = random.random() < prob_eal
        send = random.random() < prob_send
        ehcp = (random.random() < prob_ehcp_given_send) if send else False
        lac = random.random() < prob_lac
        attendance = int(random.betavariate(19, 1) * 100)
        if send:
            baseline = int(random.betavariate(5, 10) * 100)
        else:
            baseline = int(random.betavariate(10, 5) * 100)

        pp_effect = -5 if pp else 0
        attendance_effect = attendance - 100

        students.append(
            {
                "student_id": student_id,
                "school_id": school_id,
                "sex": sex,
                "ks2_maths_score": baseline + int(random.gauss(mu=0, sigma=5)),
                "ks2_reading_score": baseline + int(random.gauss(mu=0, sigma=5)),
                "pp": "T" if pp else "F",
                "eal": "T" if eal else "F",
                "send": "T" if send else "F",
                "ehcp": "T" if ehcp else "F",
                "lac": "T" if lac else "F",
            }
        )

        # Generate attendance records.
        if year_group <= 10:
            # For the one school we have data for, there are only attendance records for students in
            # year 10 and below.  Each pupil has 12 attendance records; the synthetic data contains
            # 2.
            results.append(
                {
                    "student_id": student_id,
                    "teacher_id": None,
                    "class_id": None,
                    "year_group": f"Y{year_group}",
                    "academic_year": academic_year,
                    # For the one school we have, all attendance records are recorded on this date.
                    "date": "2025-08-05",
                    "assessment_type": "#AttendanceRR Cumulative YTD Snapshot",
                    "subject": "#AttendanceRR",
                    "score": attendance,
                }
            )
            results.append(
                {
                    "student_id": student_id,
                    "teacher_id": None,
                    "class_id": None,
                    "year_group": f"Y{year_group}",
                    "academic_year": academic_year,
                    # For the one school we have, all attendance records are recorded on this date.
                    "date": "2025-08-05",
                    "assessment_type": "#AttendanceRR Half Term Snapshot",
                    "subject": "#AttendanceRR",
                    # Add a random fluctuation so that the two attendance records are different.
                    "score": min(attendance + random.gauss(mu=0, sigma=5), 100),
                }
            )

        # Generate assessment records.
        for subject in subjects:
            if year_group <= 9 and subject in nvq_subjects:
                # No NVQs in KS3.
                continue
            if random.random() < 0.1:
                # Not every student takes every subject.
                continue

            # Find a class and teacher for this year group and subject.
            cls = random.choice(classes[(year_group, subject)])

            # Generate a raw score based on the student's baseline, modified by various effects.
            raw_score = int(
                baseline
                + school_effect
                + cls["teacher"]["effect"]
                + cls["effect"]
                + pp_effect
                + attendance_effect
            )

            score = convert_score(raw_score, year_group, subject in nvq_subjects)

            results.append(
                {
                    "student_id": student_id,
                    "teacher_id": cls["teacher"]["id"],
                    "class_id": cls["id"],
                    "year_group": f"Y{year_group}",
                    "academic_year": academic_year,
                    "date": date(2025, 1, 1) + timedelta(random.randint(0, 180)),
                    "assessment_type": "*Current Grades",
                    "subject": subject,
                    "score": score,
                }
            )


def build_classes(school_ix):
    """Return dictionary mapping (year_group, subject) pairs to class metadata."""
    teachers = defaultdict(list)
    classes = defaultdict(list)

    for teacher_ix in range(num_teachers_per_school):
        teacher_id = f"TCH{school_ix:02}{teacher_ix:02}"
        subject = subjects[teacher_ix % len(subjects)]
        teachers[subject].append(
            {"id": teacher_id, "effect": random.gauss(mu=0, sigma=5)}
        )

    class_ix = 0
    for year_group in range(7, 14):
        for subject in subjects:
            if year_group <= 9 and subject in nvq_subjects:
                continue
            for teacher in teachers[subject]:
                class_id = f"CLS{school_ix:02}{class_ix:02}"
                classes[(year_group, subject)].append(
                    {
                        "id": class_id,
                        "effect": random.gauss(mu=0, sigma=20),
                        "teacher": teacher,
                    }
                )
                class_ix += 1

    return classes

def convert_score(raw_score, year_group, is_nvq):
    """Convert a raw score (between 0 and 100) into a suitable grade."""

    # Ensure the raw score is in range [0, 1).
    raw_score = max(0, min(raw_score, 99)) * 0.01

    if year_group <= 9:
        assert not is_nvq
        scores = [f"{num}{letter}" for num in "789" for letter in "ABCDE"]
    elif year_group <= 11:
        if is_nvq:
            scores = ["U", "P", "M", "D2", "D1", "D*"]
        else:
            scores = list("123456789")
    else:
        assert year_group <= 13
        if is_nvq:
            # Other values might be possible here but are not present in data for the one school we
            # have.
            scores = ["U", "PPP", "MMM", "DDM", "DDD", "D*DD", "D*D*D", "D*D*D*"]
        else:
            scores = list("UEDCBA") + ["A*"]

    return scores[int(raw_score * len(scores))]


def write_output(output_dir, results, students):
    output_dir.mkdir(parents=True, exist_ok=True)

    results_file = output_dir / "results.csv"
    students_file = output_dir / "students.csv"

    with open(students_file, "w") as f:
        writer = csv.DictWriter(f, students[0].keys())
        writer.writeheader()
        writer.writerows(students)

    with open(results_file, "w") as f:
        writer = csv.DictWriter(f, results[0].keys())
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
