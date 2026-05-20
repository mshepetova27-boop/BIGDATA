# dags/scripts/build_mart_performance.py
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

def get_db_config():
    """Читает параметры подключения из переменных окружения"""
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5433'),
        'database': os.getenv('DB_NAME', 'my_db_schepetova'),
        'user': os.getenv('DB_USER', 'schepetova'),
        'password': os.getenv('DB_PASSWORD', '972Hatw')
    }
    return config

def create_mart_performance():
    """Создает витрину dmr.analytics_student_performance"""
    conn = None
    try:
        config = get_db_config()
        print(f"Подключение к {config['host']}:{config['port']} ...")
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        
        # 1. Создание схемы dmr
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
            conn.commit()
            print("Схема dmr создана/существует")
        
        # 2. Создание таблицы (расширенная версия)
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
            print("Таблица dmr.analytics_student_performance создана")
        
        # 3. Заполнение данными (ваш сложный SELECT из задания 10)
        select_query = """
        WITH 
        student_course_base AS (
            SELECT 
                ul.userid,
                ul.courseid,
                MAX(ul.depart) AS department_id,
                MAX(ul.num_sem) AS semester,
                MAX(ul.kurs) AS course_year,
                MAX(ul.namer_level) AS final_grade,
                MAX(ul.leveled) AS education_level_raw,
                MAX(ul.name_osno) AS education_base_raw
            FROM public.user_logs ul
            WHERE ul.namer_level IS NOT NULL 
              AND ul.namer_level IN (2, 3, 4, 5)
            GROUP BY ul.userid, ul.courseid
        ),
        
        events_aggregated AS (
            SELECT 
                userid,
                courseid,
                SUM(s_all) AS total_events,
                ROUND(AVG(s_all), 2) AS avg_weekly_events,
                SUM(s_course_viewed) AS total_course_views,
                SUM(s_q_attempt_viewed) AS total_quiz_views,
                SUM(s_a_course_module_viewed) AS total_module_views,
                SUM(s_a_submission_status_viewed) AS total_submissions,
                MODE() WITHIN GROUP (ORDER BY num_week) AS peak_activity_week,
                CASE 
                    WHEN AVG(s_all) > 0 
                    THEN ROUND(GREATEST(0, 1 - STDDEV(s_all) / AVG(s_all)), 2)
                    ELSE 0
                END AS consistency_score
            FROM public.user_logs
            GROUP BY userid, courseid
        ),
        
        departments_info AS (
            SELECT id AS department_id, name AS department_name
            FROM public.departments
        ),
        
        thresholds AS (
            SELECT 
                PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY total_events) AS low_cutoff,
                PERCENTILE_CONT(0.67) WITHIN GROUP (ORDER BY total_events) AS high_cutoff
            FROM events_aggregated
        )
        
        SELECT 
            scb.userid AS student_id,
            scb.courseid AS course_id,
            scb.department_id,
            COALESCE(di.department_name, 'Не указана') AS department_name,
            CASE 
                WHEN scb.education_level_raw = '1' THEN 'Бакалавриат'
                WHEN scb.education_level_raw = '2' THEN 'Магистратура'
                ELSE COALESCE(scb.education_level_raw, 'Не указано')
            END AS education_level,
            CASE 
                WHEN scb.education_base_raw = '1' THEN 'Бюджет'
                WHEN scb.education_base_raw = '2' THEN 'Контракт'
                ELSE COALESCE(scb.education_base_raw, 'Не указано')
            END AS education_base,
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
                WHEN COALESCE(ea.total_events, 0) <= t.low_cutoff THEN 'низкая'
                WHEN COALESCE(ea.total_events, 0) <= t.high_cutoff THEN 'средняя'
                ELSE 'высокая'
            END AS activity_category
        FROM student_course_base scb
        LEFT JOIN events_aggregated ea ON scb.userid = ea.userid AND scb.courseid = ea.courseid
        LEFT JOIN departments_info di ON scb.department_id = di.department_id
        CROSS JOIN thresholds t
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
            # Проверка наличия данных
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
        
        print("Витрина dmr.analytics_student_performance успешно создана/обновлена.")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# Функция для прямого вызова (не обязательна для Airflow, но для тестов)
if __name__ == "__main__":
    create_mart_performance()