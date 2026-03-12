#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Создаем основную таблицу с правильными типами
    CREATE TABLE IF NOT EXISTS user_logs (
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
        NameR_Level INTEGER,
        Name_vAtt VARCHAR(255),
        Depart INTEGER,
        Name_OsnO INTEGER,
        Name_FormOPril INTEGER,
        LevelEd INTEGER,
        Num_Sem INTEGER,
        Kurs INTEGER,
        Date_vAtt DATE
    );

    CREATE TABLE IF NOT EXISTS departments (
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
        NameR_Level TEXT,
        Name_vAtt TEXT,
        Depart TEXT,
        Name_OsnO TEXT,
        Name_FormOPril TEXT,
        LevelEd TEXT,
        Num_Sem TEXT,
        Kurs TEXT,
        Date_vAtt TEXT
    );

    -- копируем сырые данные во временную таблицу
    \copy user_logs_temp FROM '/datasets/aggrigation_logs_per_week.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
    \copy departments FROM '/datasets/departments.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

    -- копируем данные в основную таблицу
    INSERT INTO user_logs (
        courseid, userid, num_week, s_all, s_all_avg,
        s_course_viewed, s_course_viewed_avg, s_q_attempt_viewed,
        s_q_attempt_viewed_avg, s_a_course_module_viewed,
        s_a_course_module_viewed_avg, s_a_submission_status_viewed,
        s_a_submission_status_viewed_avg, NameR_Level, Name_vAtt,
        Depart, Name_OsnO, Name_FormOPril, LevelEd, Num_Sem, Kurs, Date_vAtt
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
        NameR_Level::INTEGER,
        Name_vAtt,
        Depart::INTEGER,
        Name_OsnO::INTEGER,
        Name_FormOPril::INTEGER,
        LevelEd::INTEGER,
        Num_Sem::INTEGER,
        Kurs::INTEGER,
        to_date(Date_vAtt, 'DD.MM.YYYY')
    FROM user_logs_temp;

    -- Временная таблица удалится сама
EOSQL