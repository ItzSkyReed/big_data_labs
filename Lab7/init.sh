#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS airflow_metadata;

    -- Создаем основную таблицу с правильными типами
    CREATE TABLE IF NOT EXISTS public.user_logs (
        courseid INTEGER,
        userid INTEGER,
        num_week INTEGER,
        s_all INTEGER,
        s_all_avg FLOAT,
        s_course_viewed INTEGER,
        s_course_viewed_avg FLOAT,
        s_q_attempt_viewed INTEGER,
        s_q_attempt_viewed_avg FLOAT,
        s_a_course_module_viewed INTEGER,
        s_a_course_module_viewed_avg FLOAT,
        s_a_submission_status_viewed INTEGER,
        s_a_submission_status_viewed_avg FLOAT,
        namer_level INTEGER,
        name_vatt VARCHAR(255),
        depart INTEGER,
        name_osno INTEGER,
        name_formopril INTEGER,
        leveled INTEGER,
        num_sem INTEGER,
        kurs INTEGER,
        date_vatt DATE
    );

    CREATE TABLE IF NOT EXISTS public.departments (
        id INTEGER,
        name VARCHAR(255)
    );

    -- Создаем временную таблицу, где всё TEXT чтобы не было ошибок всяких форматов
    CREATE TEMPORARY TABLE user_logs_temp (
        courseid TEXT,
        userid TEXT,
        num_week TEXT,
        s_all TEXT,
        s_all_avg TEXT,
        s_course_viewed TEXT,
        s_course_viewed_avg TEXT,
        s_q_attempt_viewed TEXT,
        s_q_attempt_viewed_avg TEXT,
        s_a_course_module_viewed TEXT,
        s_a_course_module_viewed_avg TEXT,
        s_a_submission_status_viewed TEXT,
        s_a_submission_status_viewed_avg TEXT,
        namer_level TEXT,
        name_vatt TEXT,
        depart TEXT,
        name_osno TEXT,
        name_formopril TEXT,
        leveled TEXT,
        num_sem TEXT,
        kurs TEXT,
        date_vatt TEXT
    );

    -- копируем сырые данные во временную таблицу
    \copy user_logs_temp FROM '/datasets/aggrigation_logs_per_week.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
    \copy public.departments FROM '/datasets/departments.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

    -- копируем данные в основную таблицу
    INSERT INTO public.user_logs (
        courseid, userid, num_week, s_all, s_all_avg,
        s_course_viewed, s_course_viewed_avg, s_q_attempt_viewed,
        s_q_attempt_viewed_avg, s_a_course_module_viewed,
        s_a_course_module_viewed_avg, s_a_submission_status_viewed,
        s_a_submission_status_viewed_avg, namer_level, name_vatt,
        depart, name_osno, name_formopril, leveled, num_sem, kurs, date_vatt
    )
    SELECT
        courseid::INTEGER,
        userid::INTEGER,
        num_week::INTEGER,
        s_all::INTEGER,
        REPLACE(s_all_avg, ',', '.')::FLOAT,
        s_course_viewed::INTEGER,
        REPLACE(s_course_viewed_avg, ',', '.')::FLOAT,
        s_q_attempt_viewed::INTEGER,
        REPLACE(s_q_attempt_viewed_avg, ',', '.')::FLOAT,
        s_a_course_module_viewed::INTEGER,
        REPLACE(s_a_course_module_viewed_avg, ',', '.')::FLOAT,
        s_a_submission_status_viewed::INTEGER,
        REPLACE(s_a_submission_status_viewed_avg, ',', '.')::FLOAT,
        namer_level::INTEGER,
        name_vatt,
        depart::INTEGER,
        name_osno::INTEGER,
        name_formopril::INTEGER,
        leveled::INTEGER,
        num_sem::INTEGER,
        kurs::INTEGER,
        to_date(date_vatt, 'DD.MM.YYYY')
    FROM user_logs_temp;

EOSQL