/*
We have the following tables

* attainments (attainmentKey, studentId, assessmentId, classId, attainmentDate, score)
* students (studentKey, etc)
* assessments (assessmentKey, assessmentType, subject)
* classes (classKey, academicYear, yearGroup)
* teacherClassAllocations (classId, teacherId)

An attainment is a record that student X sat assessment Y as part of class Z on a given date.

We want to generate a table with one row per attainment and the following columns:

* student_id
* class_id
* teacher_id
* year_group
* academic_year
* date
* assessment_type
* subject
* score

There are a few wrinkles:

* An attainment can appear multiple times with the same key but different classes.
    When this happens, we pick the one with the lowest class key.
* A class can be taught by multiple teachers.
    When this happens, we set teacher_id to NULL.
* Some attainments cannot be linked to classes.
    As such, we use a LEFT JOIN from attainments to classes.
*/


WITH attainmentsWithRowNum AS (
    SELECT
        attainmentKey,
        studentId,
        classId,
        assessmentId,
        attainmentDate,
        score,
        ROW_NUMBER() OVER (PARTITION BY attainmentKey ORDER BY classId ASC) AS rn
    FROM attainments
),
dedupedAttainments AS (
    SELECT 
        attainmentKey,
        studentId,
        classId,
        assessmentId,
        attainmentDate,
        score
    FROM attainmentsWithRowNum
    WHERE rn = 1
),
classTeachers AS (
    SELECT
        tca.classId,
        CASE
            WHEN COUNT(DISTINCT tca.teacherId) = 1 THEN MAX(tca.teacherId)
            ELSE NULL
        END AS teacherId
    FROM teacherClassAllocations AS tca
    GROUP BY tca.classId
)
SELECT
    at.studentId AS student_id,
    at.classId AS class_id,
    ct.teacherId AS teacher_id,
    c.yearGroup AS year_group,
    c.academicYear AS academic_year,
    at.attainmentDate AS [date],
    a.assessmentType AS assessment_type,
    a.subject AS subject,
    at.score
FROM dedupedAttainments AS at
JOIN assessments AS a
    ON a.assessmentKey = at.assessmentId
LEFT JOIN classes AS c
    ON c.classKey = at.classId
LEFT JOIN classTeachers AS ct
    ON ct.classId = at.classId
