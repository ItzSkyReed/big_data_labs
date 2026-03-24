import os
import sys

import psycopg2
from psycopg2 import sql
from psycopg2._psycopg import connection
from psycopg2.extras import execute_values


def get_db_config():
    """Читает параметры подключения из переменных окружения"""
    config = {
        'host': 'postgres_logs_db_7',
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'user_logs_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    return config


# подключение к БД
def get_connection() -> connection:
    """Устанавливает и возвращает соединение с БД."""
    try:
        config = get_db_config()
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

def create_schema(conn: connection):
    """Создаёт схему dmr, если она ещё не существует."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS dmr
            """
        )
        conn.commit()
        print("Схема dmr создана (или уже существовала).")


def create_table(conn: connection):
    """Создаёт пустой каркас витрины."""
    query = """
            CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance
            (
                student_id         INTEGER,
                course_id          INTEGER,
                department_id      INTEGER,
                department_name    VARCHAR(255),
                education_level    VARCHAR(255),
                education_base     VARCHAR(255),
                semester           INTEGER,
                course_year        INTEGER,
                final_grade        INTEGER,
                total_events       INTEGER,
                avg_weekly_events  DECIMAL(10, 2),
                total_course_views INTEGER,
                total_quiz_views   INTEGER,
                total_module_views INTEGER,
                total_submissions  INTEGER,
                peak_activity_week INTEGER,
                consistency_score  DECIMAL(5, 2),
                activity_category  VARCHAR(255),
                last_update        TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (student_id, course_id)
            );
            """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def fill_base_ids(conn):
    """Заполняем семестр, кафедры без имени, тут проблема, что кафедр в целом может быть несколько поэтому делаем DISTINCT по (userid, courseid)"""
    query = """
            INSERT INTO dmr.analytics_student_performance (student_id, course_id, department_id, semester, course_year, final_grade)
            SELECT DISTINCT ON (userid, courseid) userid,
                                                  courseid,
                                                  depart,
                                                  num_sem,
                                                  kurs,
                                                  namer_level
            FROM public.user_logs
            ORDER BY userid, courseid, num_week DESC
            ON CONFLICT (student_id, course_id) DO NOTHING;
            """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    print("ID и базовые поля заполнены.")


def update_department_names(conn):
    """обновляем названия кафедр."""
    query = """
            UPDATE dmr.analytics_student_performance perf
            SET department_name = deps.name
            FROM public.departments deps
            WHERE perf.department_id = deps.id;
            """
    with conn.cursor() as cur: cur.execute(query)
    conn.commit()
    print("Названия кафедр обновлены.")


def update_education_info(conn):
    """Обновляем уровень и основу обучения"""
    query = """
            UPDATE dmr.analytics_student_performance perf
            SET education_level = edu_level.lvl_name,
                education_base  = edu_level.base
            FROM (SELECT userid,
                         courseid,
                         -- маппинг уровней обучения
                         MAX(CASE
                                 WHEN leveled = 1 THEN 'бакалавриат'
                                 WHEN leveled = 2 THEN 'магистратура'
                                 WHEN leveled = 3 THEN 'специалитет'
                                 WHEN leveled = 4 THEN 'аспирантура'
                                 ELSE 'не определено'
                             END) as lvl_name,
                         -- маппинг основы обучения
                         MAX(CASE
                                 WHEN name_osno = 1 THEN 'бюджет'
                                 WHEN name_osno = 2 THEN 'контракт'
                                 ELSE 'не определено'
                             END) as base
                  FROM public.user_logs
                  GROUP BY userid, courseid) edu_level
            WHERE perf.student_id = edu_level.userid
              AND perf.course_id = edu_level.courseid;
            """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    print("Поля уровня обучения заполнены")


def update_avg_weekly_events(conn):
    """
    Рассчитывает среднее количество событий в неделю.
    Делит общее количество событий на количество недель активности.
    """
    query = """
            UPDATE dmr.analytics_student_performance AS perf
            SET avg_weekly_events = CAST(perf.total_events AS DECIMAL(10, 2)) / weeks_count
            FROM (SELECT userid,
                         courseid,
                         COUNT(DISTINCT num_week) AS weeks_count
                  FROM public.user_logs
                  GROUP BY userid, courseid) AS activity_stats
            WHERE perf.student_id = activity_stats.userid
              AND perf.course_id = activity_stats.courseid;
            """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    print("Столбец avg_weekly_events обновлен.")


def update_total_events(conn):
    """Суммируем все события из всех строк лога по предмету."""
    query = """
            UPDATE dmr.analytics_student_performance perf
            SET total_events       = logs.total,
                total_course_views = logs.total_course,
                total_submissions  = logs.total_submission,
                total_quiz_views   = logs.total_quiz,
                total_module_views = logs.total_module
            FROM (SELECT userid,
                         courseid,
                         SUM(s_all)                        AS total,
                         SUM(s_course_viewed)              AS total_course,
                         SUM(s_q_attempt_viewed)           AS total_quiz,
                         SUM(s_a_course_module_viewed)     AS total_module,
                         SUM(s_a_submission_status_viewed) AS total_submission
                  FROM public.user_logs
                  GROUP BY userid, courseid) logs
            WHERE perf.student_id = logs.userid
              AND perf.course_id = logs.courseid;
            """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    print("Столбец total_events заполнен.")


def update_peak_activity_week(conn):
    """
    Определяем неделю с максимальной активностью.
    Используем s_all для поиска максимума.
    """
    query = """
            UPDATE dmr.analytics_student_performance AS perf
            SET peak_activity_week = top_weeks.num_week
            FROM (
                     -- Выбираем по одной строке для каждой пары студент-курс
                     SELECT DISTINCT ON (userid, courseid) userid,
                                                           courseid,
                                                           num_week
                     FROM public.user_logs
                     -- Сортируем по убыванию s_all, чтобы первая строка была пиковой.
                     ORDER BY userid, courseid, s_all DESC, num_week) AS top_weeks
            WHERE perf.student_id = top_weeks.userid
              AND perf.course_id = top_weeks.courseid;
            """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    print("Столбец peak_activity_week заполнен.")


def update_activity_category(conn):
    """
    Классифицируем студентов по категориям активности на основе квантилей total_events:
    - Нижние 25% -> низкая
    - Средние 50% -> средняя
    - Верхние 25% -> высокая
    """
    query = """
            WITH ranked_students AS
                     (SELECT student_id,
                             course_id,
                             -- Функция NTILE разбивает всех на 4 группы по 25% на основе общего кол-ва событий
                             NTILE(4) OVER (ORDER BY total_events) as quartile
                      FROM dmr.analytics_student_performance)
            UPDATE dmr.analytics_student_performance AS perf
            SET activity_category =
                    CASE
                        WHEN ranks.quartile = 1 THEN 'низкая'
                        WHEN ranks.quartile IN (2, 3) THEN 'средняя'
                        WHEN ranks.quartile = 4 THEN 'высокая'
                        END
            FROM ranked_students AS ranks
            WHERE perf.student_id = ranks.student_id
              AND perf.course_id = ranks.course_id;
            """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    print("Категории активности обновлены.")


def update_consistency_score(conn):
    """
    Рассчитывает коэффициент стабильности (0-1).
    (кол-во недель с s_all > 0) / (общее кол-во недель курса).
    """
    query = """
            WITH course_duration AS (
                -- Считаем, сколько всего недель
                SELECT courseid,
                       userid,
                       COUNT(DISTINCT num_week) AS total_course_weeks
                FROM public.user_logs
                GROUP BY courseid, userid),
                 student_active_weeks AS (
                     -- Считаем только те недели, где s_all > 0
                     SELECT userid,
                            courseid,
                            COUNT(DISTINCT num_week) AS active_weeks_count
                     FROM public.user_logs
                     WHERE s_all > 0
                     GROUP BY userid, courseid)
            UPDATE dmr.analytics_student_performance AS perf
            SET consistency_score = CAST(student_active_weeks.active_weeks_count AS DECIMAL(5, 2)) / total_course_weeks
            FROM student_active_weeks
                     JOIN course_duration ON student_active_weeks.courseid = course_duration.courseid
            WHERE perf.student_id = student_active_weeks.userid
              AND perf.course_id = student_active_weeks.courseid;
            """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    print("Коэффициент стабильности обновлен.")


def create_mart():
    conn = get_connection()
    try:
        # Создаем схему
        create_schema(conn)
        # Создаем таблицу
        create_table(conn)

        # Заполняем стобца
        fill_base_ids(conn)
        update_department_names(conn)
        update_education_info(conn)
        update_total_events(conn)
        update_avg_weekly_events(conn)
        update_peak_activity_week(conn)
        update_activity_category(conn)
        update_consistency_score(conn)

        print("\nВитрина полностью собрана.")
    except Exception as e:
        print(f"Ошибка: {e}")
        conn.rollback()
    finally:
        conn.close()
