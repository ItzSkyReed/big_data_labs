from datetime import datetime
from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Dataset

from sqlalchemy import text
from src.models import Base, get_session

university_raw_data = Dataset("university_app://raw_data")

@dag(
    dag_id="daily_build_group_mart",
    schedule=[university_raw_data],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mart"]
)
def build_group_mart_dag():

    @task
    def process_group_mart_in_db():
        # Открываем сессию
        with get_session() as session:

            # Проверка и создание схемы
            session.execute(text("CREATE SCHEMA IF NOT EXISTS mart;"))
            session.commit()

            # Создание таблицы витрины
            engine = session.get_bind()
            Base.metadata.create_all(engine)

            # Очищаем старые данные
            session.execute(text("TRUNCATE TABLE mart.group_stats;"))

            # Расчет метрик и вставка новых данных
            insert_query = text("""
                                INSERT INTO mart.group_stats (group_id, course, institute, avg_grade_group, pass_rate, best_subject, worst_subject)

                                -- количество долгов для каждого студента
                                WITH student_debts AS (SELECT student.id                                                              AS student_id,
                                                              student.group_id,
                                                              COUNT(CASE WHEN grades.grade IN ('2', 'Незачет', 'незачет') THEN 1 END) AS debt_count
                                                       FROM public.students student
                                                                LEFT JOIN public.grades grades ON student.id = grades.student_id
                                                       GROUP BY student.id, student.group_id),

                                     --  доля успевающих студентов (pass_rate=true) по группам
                                     group_pass_stats AS (SELECT group_id,
                                                                 CAST(COUNT(CASE WHEN debt_count = 0 THEN 1 END) AS FLOAT) /
                                                                 COUNT(student_id) AS pass_rate
                                                          FROM student_debts
                                                          GROUP BY group_id),

                                     -- средний балл группы по всем предметам в сумме
                                     group_avg_grades AS (SELECT students.group_id,
                                                                 AVG(CAST(grades.grade AS NUMERIC)) AS avg_grade_group
                                                          FROM public.students students
                                                                   JOIN public.grades grades ON students.id = grades.student_id
                                                          WHERE grades.grade IN ('2', '3', '4', '5')
                                                          GROUP BY students.group_id),

                                     -- средний балл группы по каждому предмету
                                     group_subject_grades AS (SELECT student.group_id,
                                                                     subject.name                       AS subject_name,
                                                                     AVG(CAST(grades.grade AS NUMERIC)) AS subject_avg_grade
                                                              FROM public.students student
                                                                       JOIN public.grades grades ON student.id = grades.student_id
                                                                       JOIN public.subjects subject ON grades.subject_id = subject.id
                                                              WHERE grades.grade IN ('2', '3', '4', '5')
                                                              GROUP BY student.group_id, subject.name),

                                     -- максимальный и минимальный средний балл для каждой группы
                                     min_max_grades AS (SELECT group_id,
                                                               MAX(subject_avg_grade) AS max_grade,
                                                               MIN(subject_avg_grade) AS min_grade
                                                        FROM group_subject_grades
                                                        GROUP BY group_id),

                                     -- ищем предметы, которые соответствуют этим баллам
                                     best_worst_subjects AS (SELECT group_sub_grades.group_id,
                                                                    -- Если два предмета имеют одинаковый балл, берем один из них по алфавиту
                                                                    MAX(CASE WHEN group_sub_grades.subject_avg_grade = min_max_grades.max_grade THEN group_sub_grades.subject_name END) AS best_subject,
                                                                    MAX(CASE WHEN group_sub_grades.subject_avg_grade = min_max_grades.min_grade THEN group_sub_grades.subject_name END) AS worst_subject
                                                             FROM group_subject_grades group_sub_grades
                                                                      JOIN min_max_grades ON group_sub_grades.group_id = min_max_grades.group_id
                                                             GROUP BY group_sub_grades.group_id)


                                SELECT groups.id                     AS group_id,
                                       groups.course,
                                       groups.institute,
                                       ROUND(gag.avg_grade_group, 2) AS avg_grade_group,
                                       gps.pass_rate                 AS pass_rate,
                                       bws.best_subject              AS best_subject,
                                       bws.worst_subject             AS worst_subject
                                FROM public.groups groups
                                         LEFT JOIN group_pass_stats gps ON groups.id = gps.group_id
                                         LEFT JOIN group_avg_grades gag ON groups.id = gag.group_id
                                         LEFT JOIN best_worst_subjects bws ON groups.id = bws.group_id;
                                """)

            session.execute(insert_query)

            # Фиксируем изменения
            session.commit()

        print("Витрина group_stats успешно пересчитана!")

    process_group_mart_in_db()

build_group_mart_dag()
