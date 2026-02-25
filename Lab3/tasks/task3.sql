/*
 Решите с помощью sql функций проблему некорректности данных для все всех столбцов.
 */
-- Начнем с varchar -> integer
BEGIN;

-- Вывод столбцов и их типов ДО нормализации
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'user_logs'
  AND table_schema = 'public'
ORDER BY ordinal_position;

ALTER TABLE user_logs
    ALTER COLUMN leveled TYPE integer -- приводим к числу
        USING leveled::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN leveled TYPE integer -- приводим к числу
        USING leveled::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN name_formopril TYPE integer -- приводим к числу
        USING name_formopril::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN name_osno TYPE integer -- приводим к числу
        USING name_osno::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN namer_level TYPE integer -- приводим к числу
        USING namer_level::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN s_a_submission_status_viewed TYPE integer -- приводим к числу
        USING s_a_submission_status_viewed::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN s_a_course_module_viewed TYPE integer -- приводим к числу
        USING s_a_course_module_viewed::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN s_q_attempt_viewed TYPE integer -- приводим к числу
        USING s_q_attempt_viewed::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN s_course_viewed TYPE integer -- приводим к числу
        USING s_course_viewed::integer; -- к числовому

ALTER TABLE user_logs
    ALTER COLUMN s_all TYPE integer -- приводим к числу
        USING s_all::integer; -- к числовому


-- varchar -> float
ALTER TABLE user_logs
    ALTER COLUMN s_a_submission_status_viewed_avg TYPE float -- меняем тип данных с varchar на float
        USING REPLACE(s_a_submission_status_viewed_avg, ',', '.')::float; -- приводим с запятой на точку числа в колонке и преобразовываем в float

ALTER TABLE user_logs
    ALTER COLUMN s_a_course_module_viewed_avg TYPE float -- меняем тип данных с varchar на float
        USING REPLACE(s_a_course_module_viewed_avg, ',', '.')::float; -- приводим с запятой на точку числа в колонке и преобразовываем в float

ALTER TABLE user_logs
    ALTER COLUMN s_q_attempt_viewed_avg TYPE float -- меняем тип данных с varchar на float
        USING REPLACE(s_q_attempt_viewed_avg, ',', '.')::float; -- приводим с запятой на точку числа в колонке и преобразовываем в float

ALTER TABLE user_logs
    ALTER COLUMN s_all_avg TYPE float -- меняем тип данных с varchar на float
        USING REPLACE(s_all_avg, ',', '.')::float; -- приводим с запятой на точку числа в колонке и преобразовываем в float

ALTER TABLE user_logs
    ALTER COLUMN s_course_viewed_avg TYPE float -- меняем тип данных с varchar на float
        USING REPLACE(s_course_viewed_avg, ',', '.')::float; -- приводим с запятой на точку числа в колонке и преобразовываем в float


-- varchar -> date
ALTER TABLE user_logs
    ALTER COLUMN date_vatt TYPE date -- приводим к числу
        USING TO_DATE(date_vatt, 'DD.MM.YYYY'); -- к дате

-- Вывод столбцов и их типов после нормализации
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'user_logs'
  AND table_schema = 'public'
ORDER BY ordinal_position;

ROLLBACK;