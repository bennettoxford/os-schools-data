# OpenSAFELY Schools Data

This repo contains scripts for working with data for the [OpenSAFELY schools](https://schools.opensafely.org/) project.

## `sql/students.sql`, `sql/results.sql`

These SQL scripts extract data from the TED database in a structure that conforms to the [ehrQL schema for TED data](https://docs.google.com/document/d/1vxEM9V6J28TtwQwDGRG861ApW-D6RLT0rJs2OJ_NSh0).

## `generate_synthetic_data.py`

This script generates CSV files of synthetic data that can be used for developing research code at arm's length from real data.

The data is based on our understanding of the data that will be available in TED, which is derived from exploring the data from one school that is currently available in TED.
As data from more schools is added to TED, the script may need to change.

The schema is described in [this doc](https://docs.google.com/document/d/1vxEM9V6J28TtwQwDGRG861ApW-D6RLT0rJs2OJ_NSh0).

We make several simplifying assumptions, including:

* Each teacher only teaches one subject.
* Each subject is equally popular among students.
* There is a single result (with `assessment_type` "\*Current Grades") for each student for each class they take.

These assumptions can be revisited if it turns out that the synthetic data needs closer fidelity to the real thing.

A student's performance in an assessment is derived from their baseline score, modified by school-, class-, and teacher-level effects as well as their PP status and their attendance.

Find the CSV files in the synthetic-data/ directory.

## `generate_report.py`

This script generates a report about either the synthetic data (from `generate_synthetic_data.py`) or the real data (from the SQL scripts).

Find reports for the synthetic and the real data in the reports/ directory.
