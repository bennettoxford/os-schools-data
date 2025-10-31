SELECT
    studentKey AS student_id,
    schoolId AS school_id,
    sex,
    ks2MathsScaledScore AS ks2_maths_score,
    ks2ReadingScaledScore AS ks2_reading_score,
    CASE WHEN isPupilPremium = 1 THEN 'T' ELSE 'F' END AS pp,
    CASE WHEN isEnglishAdditionalLanguage = 1 THEN 'T' ELSE 'F' END AS eal,
    CASE WHEN specialEducationalNeeds IN ('K', 'E') THEN 'T' ELSE 'F' END AS send,
    CASE WHEN specialEducationalNeeds = 'E' THEN 'T' ELSE 'F' END AS ehcp,
    CASE WHEN isLookedAfterChild = 1 THEN 'T' ELSE 'F' END AS lac
FROM students
