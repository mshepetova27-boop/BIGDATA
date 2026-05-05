"""
Требуется создать отдельную схему dmr (Data Mart Repository) для аналитических данных и 
разместить в ней витрину analytics_student_performance.

Требования:
- Создать схему dmr если она не существует
- Создать витрину dmr.analytics_student_performance с агрегированными данными.
- Реализация через функции
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Загружаем переменные из .env файла (если он есть)
load_dotenv()


def get_db_config():
    """
    Формирует словарь с параметрами подключения к БД.    
    """
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5433'),
        'database': os.getenv('DB', 'my_db_schepetova'),
        'user': os.getenv('USER', 'schepetova'),
        'password': os.getenv('PASSWORD', '972Hatw')
    }  
    print(config)
    return config


def get_connection():
    """Устанавливает и возвращает соединение с БД."""
    try:
        config = get_db_config()
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)


def create_schema(conn):
    """Создаёт схему dmr, если она ещё не существует."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
        conn.commit()
        print("Схема dmr успешно создана (или уже существовала).")


def create_table(conn):
    """Создаёт таблицу dmr.analytics_student_performance с заданной структурой."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance (
        student_id          INTEGER NOT NULL,
        course_id           INTEGER NOT NULL,
        department_id       INTEGER,
        department_name     VARCHAR(200),
        education_level     VARCHAR(50),
        education_base      VARCHAR(50),
        semester            INTEGER,
        course_year         INTEGER,
        final_grade         INTEGER CHECK (final_grade IN (2,3,4,5)),
        total_events        INTEGER DEFAULT 0,
        avg_weekly_events   DECIMAL(10,2) DEFAULT 0,
        total_course_views  INTEGER DEFAULT 0,
        total_quiz_views    INTEGER DEFAULT 0,
        total_module_views  INTEGER DEFAULT 0,
        total_submissions   INTEGER DEFAULT 0,
        peak_activity_week  INTEGER,
        consistency_score   DECIMAL(5,2) DEFAULT 0,
        activity_category   VARCHAR(20),
        last_update         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (student_id, course_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        print("Таблица dmr.analytics_student_performance успешно создана.")


def insert_data(conn):
    """
    Заполняет витрину dmr.analytics_student_performance агрегированными данными 
    из public.user_logs и связанных таблиц.
    """
    
    # Адаптированный запрос под структуру ваших данных
    select_query = """
    WITH 
    -- Основные данные о студентах и курсах
    student_course_base AS (
        SELECT 
            ul.userid,
            ul.courseid,
            MAX(ul.depart) AS department_id,
            MAX(ul.num_sem) AS semester,
            MAX(ul.kurs) AS course_year,
            MAX(ul.namer_level) AS final_grade
        FROM public.user_logs ul
        WHERE ul.namer_level IS NOT NULL 
          AND ul.namer_level IN (2, 3, 4, 5)
        GROUP BY ul.userid, ul.courseid
    ),
    
    -- Агрегация событий по студенту и курсу
    events_aggregated AS (
        SELECT 
            userid,
            courseid,
            -- Всего событий за семестр (суммируем s_all по всем неделям)
            SUM(s_all) AS total_events,
            -- Среднее событий в неделю
            ROUND(AVG(s_all), 2) AS avg_weekly_events,
            -- Всего просмотров курса
            SUM(s_course_viewed) AS total_course_views,
            -- Всего просмотров тестов
            SUM(s_q_attempt_viewed) AS total_quiz_views,
            -- Всего просмотров модулей
            SUM(s_a_course_module_viewed) AS total_module_views,
            -- Всего отправленных заданий (статус просмотра = отправленные)
            SUM(s_a_submission_status_viewed) AS total_submissions,
            -- Неделя с максимальной активностью
            MODE() WITHIN GROUP (ORDER BY num_week) AS peak_activity_week,
            -- Коэффициент стабильности активности (1 - отношение разброса к среднему)
            CASE 
                WHEN AVG(s_all) > 0 
                THEN ROUND(GREATEST(0, LEAST(1, 1 - (STDDEV(s_all) / NULLIF(AVG(s_all), 0)))), 2)
                ELSE 0
            END AS consistency_score
        FROM public.user_logs
        GROUP BY userid, courseid
    ),
    
    -- Справочник кафедр
    departments_info AS (
        SELECT 
            id AS department_id,
            name AS department_name
        FROM public.departments
    )
    
    -- Финальный SELECT
    SELECT 
        scb.userid AS student_id,
        scb.courseid AS course_id,
        scb.department_id,
        COALESCE(di.department_name, 'Не указана') AS department_name,
        -- Так как таблицы students нет, заполняем значениями по умолчанию
        'Бакалавриат' AS education_level,
        'Бюджет' AS education_base,
        COALESCE(scb.semester, 1) AS semester,
        COALESCE(scb.course_year, 1) AS course_year,
        scb.final_grade,
        COALESCE(ea.total_events, 0) AS total_events,
        COALESCE(ea.avg_weekly_events, 0) AS avg_weekly_events,
        COALESCE(ea.total_course_views, 0) AS total_course_views,
        COALESCE(ea.total_quiz_views, 0) AS total_quiz_views,
        COALESCE(ea.total_module_views, 0) AS total_module_views,
        COALESCE(ea.total_submissions, 0) AS total_submissions,
        ea.peak_activity_week,
        COALESCE(ea.consistency_score, 0) AS consistency_score,
        CASE 
            WHEN COALESCE(ea.total_events, 0) = 0 THEN 'низкая'
            WHEN ea.total_events <= 50 THEN 'низкая'
            WHEN ea.total_events <= 150 THEN 'средняя'
            ELSE 'высокая'
        END AS activity_category
    FROM student_course_base scb
    LEFT JOIN events_aggregated ea ON scb.userid = ea.userid AND scb.courseid = ea.courseid
    LEFT JOIN departments_info di ON scb.department_id = di.department_id
    WHERE scb.final_grade IS NOT NULL;
    """
    
    insert_query = sql.SQL("""
        INSERT INTO dmr.analytics_student_performance 
        (student_id, course_id, department_id, department_name, education_level, 
         education_base, semester, course_year, final_grade, total_events, 
         avg_weekly_events, total_course_views, total_quiz_views, total_module_views, 
         total_submissions, peak_activity_week, consistency_score, activity_category)
        VALUES %s
        ON CONFLICT (student_id, course_id) 
        DO UPDATE SET
            department_id       = EXCLUDED.department_id,
            department_name     = EXCLUDED.department_name,
            education_level     = EXCLUDED.education_level,
            education_base      = EXCLUDED.education_base,
            semester            = EXCLUDED.semester,
            course_year         = EXCLUDED.course_year,
            final_grade         = EXCLUDED.final_grade,
            total_events        = EXCLUDED.total_events,
            avg_weekly_events   = EXCLUDED.avg_weekly_events,
            total_course_views  = EXCLUDED.total_course_views,
            total_quiz_views    = EXCLUDED.total_quiz_views,
            total_module_views  = EXCLUDED.total_module_views,
            total_submissions   = EXCLUDED.total_submissions,
            peak_activity_week  = EXCLUDED.peak_activity_week,
            consistency_score   = EXCLUDED.consistency_score,
            activity_category   = EXCLUDED.activity_category,
            last_update         = CURRENT_TIMESTAMP;
    """)
    
    with conn.cursor() as cur:
        # Проверим количество уникальных студентов и курсов
        cur.execute("""
            SELECT COUNT(DISTINCT userid || '_' || courseid) 
            FROM public.user_logs 
            WHERE namer_level IS NOT NULL AND namer_level IN (2,3,4,5);
        """)
        students_count = cur.fetchone()[0]
        print(f"Найдено уникальных пар (студент, курс) с оценками: {students_count}")
        
        if students_count == 0:
            print("Нет данных для вставки.")
            return
        
        cur.execute(select_query)
        rows = cur.fetchall()
        
        if not rows:
            print("Нет данных для вставки после агрегации.")
            return
        
        data_tuples = [tuple(row) for row in rows]
        execute_values(cur, insert_query, data_tuples, page_size=1000)
        conn.commit()        
        print(f"Витрина заполнена. Добавлено/обновлено записей: {len(data_tuples)}")


def refresh_mart(conn):
    """
    Функция для полного обновления витрины (удаление старых данных и вставка новых).
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE dmr.analytics_student_performance;")
        conn.commit()
        print("Старые данные очищены.")
    
    insert_data(conn)
    print("Витрина обновлена.")


def main():
    """Последовательное выполнение шагов."""
    conn = None
    try:
        conn = get_connection()
        create_schema(conn)
        create_table(conn)
        insert_data(conn)
        print("\n✅ Все операции выполнены успешно!")
        
        # Выведем статистику
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dmr.analytics_student_performance;")
            count = cur.fetchone()[0]
            print(f"\n📊 Итоговое количество записей в витрине: {count}")
            
            if count > 0:
                cur.execute("""
                    SELECT 
                        activity_category,
                        COUNT(*) as cnt,
                        ROUND(AVG(final_grade), 2) as avg_grade,
                        ROUND(AVG(consistency_score), 2) as avg_consistency
                    FROM dmr.analytics_student_performance
                    GROUP BY activity_category
                    ORDER BY 
                        CASE activity_category
                            WHEN 'низкая' THEN 1
                            WHEN 'средняя' THEN 2
                            WHEN 'высокая' THEN 3
                        END;
                """)
                print("\n📈 Статистика по категориям активности:")
                for row in cur.fetchall():
                    print(f"  {row[0]}: {row[1]} студентов, средняя оценка: {row[2]}, стабильность: {row[3]}")
                    
    except Exception as e:
        print(f"❌ Ошибка в процессе выполнения: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("🔒 Соединение с БД закрыто.")


if __name__ == "__main__":
    main()