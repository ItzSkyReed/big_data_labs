from datetime import datetime
from airflow.sdk import dag, task

from sqlalchemy import text
# Импортируем ваши модели (предполагается, что абсолютный импорт настроен)
from src.models import Base, engine

@dag(
    dag_id="daily_build_student_mart",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mart"]
)
def build_student_mart_dag():

    @task
    def process_mart():

        # проверка и создание схемы mart если её еще не создали
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart;"))

        # создание таблицы витрины (если её еще нет)
        Base.metadata.create_all(engine)

        query = text("""
                     TRUNCATE TABLE mart.student_performance;

            -- Шаг Б: Считаем метрики и вставляем в витрину
                     INSERT INTO mart.student_performance (student_id, group_id, avg_grade, total_credits, debt_count, scholarship_eligible)

                     WITH student_metrics AS (SELECT students.id       AS student_id,
                                                     students.group_id AS group_id,

                                                     -- Средний балл (считаем только строчные 2,3,4,5 преобразовывая в число и игнорируем зачет/незачет)
                                                     AVG(
                                                             CASE
                                                                 WHEN grades.grade IN ('2', '3', '4', '5') THEN CAST(grades.grade AS NUMERIC)
                                                                 END
                                                     )                 AS avg_grade,

                                                     -- Сумма часов по успешно сданным предметам
                                                     SUM(
                                                             CASE
                                                                 WHEN grades.grade IN ('3', '4', '5', 'Зачет', 'зачет') THEN subjects.hours
                                                                 ELSE 0
                                                                 END
                                                     )                 AS total_credits,

                                                     -- Количество долгов (2 или незачет)
                                                     COUNT(
                                                             CASE
                                                                 WHEN grades.grade IN ('2', 'Незачет', 'незачет') THEN 1
                                                                 END
                                                     )                 AS debt_count

                                              FROM public.students students
                                                       LEFT JOIN public.grades grades ON students.id = grades.student_id
                                                       LEFT JOIN public.subjects subjects ON grades.subject_id = subjects.id
                                              GROUP BY students.id, students.group_id)

                     SELECT student_id,
                            group_id,
                            ROUND(avg_grade, 2)                             AS avg_grade,
                            total_credits,
                            debt_count,
                            -- нет долгов и средний балл >= 4 - есть стипендия, иначе нету
                            (debt_count = 0 AND ROUND(avg_grade, 2) >= 4.0) AS scholarship_eligible

                     FROM student_metrics;
                     """)

        with engine.begin() as conn:
            conn.execute(query)

        print("Витрина успешно пересчитана средствами PostgreSQL!")

    # Вызов таски
    process_mart()

# Регистрация DAG
build_student_mart_dag()