SELECT
    studentKey AS student_id,
    schoolId AS school_id,
    sex,
    TRY_CAST(ks2MathsScaledScore AS DECIMAL(5, 1)) AS ks2_maths_score,
    TRY_CAST(ks2ReadingScaledScore AS DECIMAL(5, 1)) AS ks2_reading_score,
    CASE WHEN isPupilPremium = 1 THEN 'T' ELSE 'F' END AS pp,
    CASE WHEN isEnglishAdditionalLanguage = 1 THEN 'T' ELSE 'F' END AS eal,
    CASE WHEN specialEducationalNeeds IN ('K', 'E') THEN 'T' ELSE 'F' END AS send,
    CASE WHEN specialEducationalNeeds = 'E' THEN 'T' ELSE 'F' END AS ehcp,
    CASE WHEN isLookedAfterChild = 1 THEN 'T' ELSE 'F' END AS lac
FROM students
