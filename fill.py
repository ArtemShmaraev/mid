import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

from config import DB_CONFIG

# Настройки подключения к БД


# Инициализация Faker
fake = Faker()


def create_connection():
    """Создает соединение с базой данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None


def generate_test_data():
    """Генерирует тестовые данные для базы данных"""
    conn = create_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        # Очистка таблиц перед заполнением (опционально)
        cursor.execute("TRUNCATE TABLE fact_student_progress CASCADE")
        cursor.execute("TRUNCATE TABLE dim_course_registrations CASCADE")
        cursor.execute("TRUNCATE TABLE dim_learning_elements CASCADE")
        cursor.execute("TRUNCATE TABLE dim_lessons CASCADE")
        cursor.execute("TRUNCATE TABLE dim_modules CASCADE")
        cursor.execute("TRUNCATE TABLE dim_courses CASCADE")
        cursor.execute("TRUNCATE TABLE dim_students CASCADE")
        cursor.execute("TRUNCATE TABLE dim_topics CASCADE")
        cursor.execute("TRUNCATE TABLE dim_element_types CASCADE")
        cursor.execute("TRUNCATE TABLE dim_dates CASCADE")
        conn.commit()

        # 1. Заполнение таблицы типов элементов
        element_types = [
            ('text', 'Текстовый учебный материал'),
            ('image', 'Изображение'),
            ('animation', 'Анимация'),
            ('video', 'Видео'),
            ('audio', 'Аудио'),
            ('test', 'Тест с закрытыми вопросами'),
            ('open_form', 'Форма для открытого ответа')
        ]
        for type_name, description in element_types:
            cursor.execute(
                "INSERT INTO dim_element_types (type_name, description) VALUES (%s, %s)",
                (type_name, description))
            conn.commit()

            # 2. Заполнение таблицы тем
            # Проверяем, существует ли уже тема Hadoop
            cursor.execute("SELECT COUNT(*) FROM dim_topics WHERE topic_name = 'Hadoop'")
            hadoop_exists = cursor.fetchone()[0] > 0

            if not hadoop_exists:
                cursor.execute(
                    "INSERT INTO dim_topics (topic_name, description) VALUES (%s, %s) RETURNING topic_id",
                    ('Hadoop', 'Все о технологии Hadoop'))
                hadoop_topic_id = cursor.fetchone()[0]
            else:
                # Если тема уже существует, получаем ее ID
                cursor.execute("SELECT topic_id FROM dim_topics WHERE topic_name = 'Hadoop'")
                hadoop_topic_id = cursor.fetchone()[0]

            # Создаем подтемы Hadoop только если они еще не существуют
            hadoop_subtopics = [
                ('HDFS', 'Hadoop Distributed File System'),
                ('MapReduce', 'MapReduce programming model'),
                ('YARN', 'Yet Another Resource Negotiator'),
                ('HBase', 'Distributed NoSQL database'),
                ('Hive', 'Data warehouse infrastructure')
            ]

            for topic_name, description in hadoop_subtopics:
                cursor.execute(
                    "SELECT COUNT(*) FROM dim_topics WHERE topic_name = %s AND parent_topic_id = %s",
                    (topic_name, hadoop_topic_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute(
                        "INSERT INTO dim_topics (topic_name, description, parent_topic_id) VALUES (%s, %s, %s)",
                        (topic_name, description, hadoop_topic_id))

            # Создаем другие темы, только если они еще не существуют
            other_topics = [
                ('Python', 'Python programming language'),
                ('SQL', 'Structured Query Language'),
                ('Machine Learning', 'Machine learning algorithms'),
                ('Data Science', 'Data science techniques'),
                ('Big Data', 'Big data technologies')
            ]

            for topic_name, description in other_topics:
                cursor.execute("SELECT COUNT(*) FROM dim_topics WHERE topic_name = %s", (topic_name,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute(
                        "INSERT INTO dim_topics (topic_name, description) VALUES (%s, %s)",
                        (topic_name, description))
            conn.commit()

            # 3. Заполнение таблицы студентов (100 студентов)
            students = []
            for _ in range(100):
                first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.unique.email()
            registration_date = fake.date_time_between(start_date='-2y', end_date='now')
            last_login = fake.date_time_between(start_date=registration_date, end_date='now')

            cursor.execute(
                "INSERT INTO dim_students (first_name, last_name, email, registration_date, last_login) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING student_id",
                (first_name, last_name, email, registration_date, last_login))
            students.append(cursor.fetchone()[0])
            conn.commit()

            # 4. Заполнение таблицы курсов
            courses = [
                ('Hadoop Fundamentals', 'Основы работы с Hadoop', True),
                ('Advanced Hadoop', 'Продвинутые техники работы с Hadoop', True),
                ('Big Data Analytics', 'Анализ больших данных', True),
                ('Data Science with Python', 'Наука о данных на Python', True),
                ('SQL for Data Analysis', 'SQL для анализа данных', True),
                ('Machine Learning Basics', 'Основы машинного обучения', False)
            ]
            course_ids = []
            for course_name, description, is_active in courses:
                creation_date = fake.date_time_between(start_date='-3y', end_date='-6m')
            cursor.execute(
                "INSERT INTO dim_courses (course_name, description, creation_date, is_active) "
                "VALUES (%s, %s, %s, %s) RETURNING course_id",
                (course_name, description, creation_date, is_active))
            course_ids.append(cursor.fetchone()[0])
            conn.commit()

            # 5. Заполнение таблицы модулей (3-5 модулей на курс)
            module_ids = []
            for course_id in course_ids:
                num_modules = random.randint(3, 5)
            for i in range(1, num_modules + 1):
                module_name = f"Module {i}"
            cursor.execute(
                "INSERT INTO dim_modules (course_id, module_name, module_order) "
                "VALUES (%s, %s, %s) RETURNING module_id",
                (course_id, module_name, i))
            module_ids.append(cursor.fetchone()[0])
            conn.commit()

            # 6. Заполнение таблицы уроков (3-8 уроков на модуль)
            lesson_ids = []
            for module_id in module_ids:
                num_lessons = random.randint(3, 8)
            for i in range(1, num_lessons + 1):
                lesson_name = f"Lesson {i}"
            # Выбираем случайную тему, но с уклоном в Hadoop для некоторых курсов
            cursor.execute("SELECT topic_id FROM dim_topics ORDER BY RANDOM() LIMIT 1")
            topic_id = cursor.fetchone()[0]

            cursor.execute(
                "INSERT INTO dim_lessons (module_id, lesson_name, lesson_order, topic_id) "
                "VALUES (%s, %s, %s, %s) RETURNING lesson_id",
                (module_id, lesson_name, i, topic_id))
            lesson_ids.append(cursor.fetchone()[0])
            conn.commit()

            # 7. Заполнение таблицы учебных элементов (3-10 элементов на урок)
            element_ids = []
            for lesson_id in lesson_ids:
                num_elements = random.randint(3, 10)
            for i in range(1, num_elements + 1):
            # Выбираем случайный тип элемента
                cursor.execute("SELECT type_id FROM dim_element_types ORDER BY RANDOM() LIMIT 1")
            element_type_id = cursor.fetchone()[0]

            difficulty_level = random.randint(1, 5)
            title = f"Element {i}"
            is_required = random.choice([True, False])
            content_url = "https://example.com/content/" + fake.uri_path()

            cursor.execute(
                "INSERT INTO dim_learning_elements (lesson_id, element_type_id, difficulty_level, "
                "element_order, title, is_required, content_url) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING element_id",
                (lesson_id, element_type_id, difficulty_level, i, title, is_required, content_url))
            element_ids.append(cursor.fetchone()[0])
            conn.commit()

            # 8. Заполнение таблицы регистраций на курсы (каждый студент регистрируется на 1-3 курса)
            registration_ids = []
            for student_id in students:
                num_courses = random.randint(1, min(3, len(course_ids)))
            selected_courses = random.sample(course_ids, num_courses)

            for course_id in selected_courses:
                registration_date = fake.date_time_between(start_date='-1y', end_date='now')
            completion_date = None
            if random.random() > 0.7:  # 30% chance of completion
                completion_date = fake.date_time_between(start_date=registration_date, end_date='now')

            is_active = completion_date is None

            try:
                cursor.execute(
                    "INSERT INTO dim_course_registrations (student_id, course_id, registration_date, "
                    "completion_date, is_active) VALUES (%s, %s, %s, %s, %s) RETURNING registration_id",
                    (student_id, course_id, registration_date, completion_date, is_active))
                registration_ids.append(cursor.fetchone()[0])
            except psycopg2.IntegrityError:
                conn.rollback()  # Пропускаем дубликаты регистраций
        conn.commit()

        # 9. Заполнение таблицы дат
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2023, 12, 31)
        current_date = start_date

        while current_date <= end_date:
            day_of_week = current_date.weekday() + 1  # 1-7
            day_name = current_date.strftime('%A')
            day_of_month = current_date.day
            day_of_year = current_date.timetuple().tm_yday
            week_of_year = current_date.isocalendar()[1]
            month_number = current_date.month
            month_name = current_date.strftime('%B')
            quarter = (current_date.month - 1) // 3 + 1
            year = current_date.year
            is_weekend = day_of_week in [6, 7]

            cursor.execute(
                "INSERT INTO dim_dates (full_date, day_of_week, day_name, day_of_month, day_of_year, "
                "week_of_year, month_number, month_name, quarter, year, is_weekend) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (current_date.date(), day_of_week, day_name, day_of_month, day_of_year,
                 week_of_year, month_number, month_name, quarter, year, is_weekend))

            current_date += timedelta(days=1)
        conn.commit()

        # 10. Заполнение фактовой таблицы прогресса студентов
        for registration_id in registration_ids:
            cursor.execute(
                "SELECT student_id, course_id FROM dim_course_registrations WHERE registration_id = %s",
                (registration_id,))
            student_id, course_id = cursor.fetchone()

            # Получаем все элементы курса
            cursor.execute("""
                SELECT le.element_id, le.lesson_id, m.module_id, le.difficulty_level, le.element_type_id
                FROM dim_learning_elements le
                JOIN dim_lessons l ON le.lesson_id = l.lesson_id
                JOIN dim_modules m ON l.module_id = m.module_id
                WHERE m.course_id = %s
            """, (course_id,))
            course_elements = cursor.fetchall()

            # Случайное количество элементов, которые студент изучил (50-100%)
            min_elements = max(1, int(len(course_elements) * 0.5))
            max_elements = len(course_elements)
            num_elements_to_complete = random.randint(min_elements, max_elements)

            selected_elements = random.sample(course_elements, num_elements_to_complete)

            for element in selected_elements:
                element_id, lesson_id, module_id, difficulty_level, element_type_id = element

            # Получаем дату регистрации на курс
            cursor.execute(
                "SELECT registration_date FROM dim_course_registrations "
                "WHERE student_id = %s AND course_id = %s",
                (student_id, course_id))
            registration_date = cursor.fetchone()[0]

            start_time = fake.date_time_between(start_date=registration_date, end_date='now')
            end_time = fake.date_time_between(start_date=start_time, end_date='now')
            duration_seconds = (end_time - start_time).total_seconds()
            score = random.uniform(0, 100) if element_type_id == 6 else None  # Оценка только для тестов
            is_completed = True
            is_required = random.choice([True, False])

            cursor.execute(
                "INSERT INTO fact_student_progress (student_id, course_id, module_id, lesson_id, "
                "element_id, start_time, end_time, duration_seconds, score, is_completed, is_required) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (student_id, course_id, module_id, lesson_id, element_id, start_time,
                 end_time, duration_seconds, score, is_completed, is_required))
            conn.commit()

            print("Тестовые данные успешно созданы!")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при создании тестовых данных: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    generate_test_data()