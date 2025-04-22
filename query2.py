import psycopg2
from psycopg2 import sql

from config import DB_CONFIG


def execute_query(query, params=None):
    """Выполняет SQL запрос и возвращает результат"""
    conn = None
    try:
        # Подключаемся к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Выполняем запрос
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Получаем результаты
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            return columns, results
        else:
            conn.commit()
            return None, f"Query executed successfully. Rows affected: {cursor.rowcount}"

    except Exception as e:
        return None, f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()


# Запрос 1: Максимальное время на уроки Hadoop
query1 = """
SELECT
    EXTRACT(YEAR FROM dcr.registration_date) AS reg_year,
    EXTRACT(MONTH FROM dcr.registration_date) AS reg_month,
    dl.lesson_id,
    MAX(fsp.duration_seconds) AS max_duration
FROM fact_student_progress fsp
JOIN dim_learning_elements dle ON fsp.element_id = dle.element_id
JOIN dim_lessons dl ON fsp.lesson_id = dl.lesson_id
JOIN dim_topics dt ON dl.topic_id = dt.topic_id
JOIN dim_course_registrations dcr ON fsp.student_id = dcr.student_id AND fsp.course_id = dcr.course_id
WHERE
    dle.difficulty_level > 3
    AND dle.element_type_id = (
        SELECT type_id FROM dim_element_types WHERE type_name = 'video'
    )
    AND LOWER(dt.topic_name) LIKE '%hadoop%'
GROUP BY reg_year, reg_month, dl.lesson_id
ORDER BY reg_year, reg_month;

"""

# Запрос 2: Популярные темы среди студентов прошлого года
query2 = """
WITH student_topic_counts AS (
    SELECT
        dl.topic_id,
        COUNT(DISTINCT fsp.student_id) AS student_count
    FROM fact_student_progress fsp
    JOIN dim_lessons dl ON fsp.lesson_id = dl.lesson_id
    JOIN dim_course_registrations dcr ON fsp.student_id = dcr.student_id AND fsp.course_id = dcr.course_id
    WHERE EXTRACT(YEAR FROM dcr.registration_date) = EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY dl.topic_id
),
max_count AS (
    SELECT MAX(student_count) AS max_student_count FROM student_topic_counts
)
SELECT dt.topic_id, dt.topic_name
FROM student_topic_counts stc
JOIN max_count mc ON stc.student_count = mc.max_student_count
JOIN dim_topics dt ON stc.topic_id = dt.topic_id;

"""

if __name__ == "__main__":
    print("Задача 1: Максимальное время на уроки Hadoop по месяцам регистрации")
    columns, results = execute_query(query1)
    if columns:
        print("\n".join(["\t".join(map(str, row)) for row in [columns] + results]))

    print("\nЗадача 2: Популярные темы среди студентов прошлого года")
    columns, results = execute_query(query2)
    if columns:
        print("\n".join(["\t".join(map(str, row)) for row in [columns] + results]))