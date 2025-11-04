/*
The teachers table contains duplicates, with one row per (teacherKey, specialism).
Apart from specialism, all values for a given teacherKey are the same.
*/

SELECT
    teacherKey AS teacher_id,
    -- payscale is recorded as eg `L03 11.0`; we just want `L03`.
    MAX(LEFT(payscale, CHARINDEX(' ', payscale + ' ') - 1)) AS payscale,
    STRING_AGG(specialism, '; ') WITHIN GROUP (ORDER BY specialism) AS specialism,
    MAX(DATEFROMPARTS(YEAR(dateStartedSchool), MONTH(dateStartedSchool), 1)) AS date_started_school
FROM teachers
GROUP BY teacherKey
