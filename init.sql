-- Фактовая таблица (центральная)
CREATE TABLE fact_student_progress (
    progress_id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    module_id INTEGER NOT NULL,
    lesson_id INTEGER NOT NULL,
    element_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    score DECIMAL(5,2),
    is_completed BOOLEAN DEFAULT FALSE,
    is_required BOOLEAN DEFAULT FALSE,
    CONSTRAINT fk_student FOREIGN KEY (student_id) REFERENCES dim_students(student_id),
    CONSTRAINT fk_course FOREIGN KEY (course_id) REFERENCES dim_courses(course_id),
    CONSTRAINT fk_module FOREIGN KEY (module_id) REFERENCES dim_modules(module_id),
    CONSTRAINT fk_lesson FOREIGN KEY (lesson_id) REFERENCES dim_lessons(lesson_id),
    CONSTRAINT fk_element FOREIGN KEY (element_id) REFERENCES dim_learning_elements(element_id)
);

-- Таблица студентов (измерение)
CREATE TABLE dim_students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    registration_date TIMESTAMP NOT NULL,
    last_login TIMESTAMP
);

-- Таблица курсов (измерение)
CREATE TABLE dim_courses (
    course_id SERIAL PRIMARY KEY,
    course_name VARCHAR(255) NOT NULL,
    description TEXT,
    creation_date TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

-- Таблица модулей (измерение)
CREATE TABLE dim_modules (
    module_id SERIAL PRIMARY KEY,
    course_id INTEGER NOT NULL,
    module_name VARCHAR(255) NOT NULL,
    module_order INTEGER NOT NULL,
    CONSTRAINT fk_course FOREIGN KEY (course_id) REFERENCES dim_courses(course_id)
);

-- Таблица уроков (измерение)
CREATE TABLE dim_lessons (
    lesson_id SERIAL PRIMARY KEY,
    module_id INTEGER NOT NULL,
    lesson_name VARCHAR(255) NOT NULL,
    lesson_order INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    CONSTRAINT fk_module FOREIGN KEY (module_id) REFERENCES dim_modules(module_id),
    CONSTRAINT fk_topic FOREIGN KEY (topic_id) REFERENCES dim_topics(topic_id)
);

-- Таблица учебных элементов (измерение)
CREATE TABLE dim_learning_elements (
    element_id SERIAL PRIMARY KEY,
    lesson_id INTEGER NOT NULL,
    element_type_id INTEGER NOT NULL,
    difficulty_level INTEGER CHECK (difficulty_level BETWEEN 1 AND 5),
    element_order INTEGER NOT NULL,
    title VARCHAR(255) NOT NULL,
    is_required BOOLEAN DEFAULT FALSE,
    content_url TEXT,
    CONSTRAINT fk_lesson FOREIGN KEY (lesson_id) REFERENCES dim_lessons(lesson_id),
    CONSTRAINT fk_element_type FOREIGN KEY (element_type_id) REFERENCES dim_element_types(type_id)
);

-- Таблица типов элементов (измерение)
CREATE TABLE dim_element_types (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255)
);

-- Таблица тем (измерение)
CREATE TABLE dim_topics (
    topic_id SERIAL PRIMARY KEY,
    topic_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    parent_topic_id INTEGER,
    CONSTRAINT fk_parent_topic FOREIGN KEY (parent_topic_id) REFERENCES dim_topics(topic_id)
);

-- Таблица регистраций на курсы (измерение)
CREATE TABLE dim_course_registrations (
    registration_id SERIAL PRIMARY KEY,
    student_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    registration_date TIMESTAMP NOT NULL,
    completion_date TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT fk_student FOREIGN KEY (student_id) REFERENCES dim_students(student_id),
    CONSTRAINT fk_course FOREIGN KEY (course_id) REFERENCES dim_courses(course_id),
    CONSTRAINT unique_registration UNIQUE (student_id, course_id)
);

-- Таблица дат (измерение для анализа по времени)
CREATE TABLE dim_dates (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);


-- Индексы для фактовой таблицы
CREATE INDEX idx_fact_student_progress_student ON fact_student_progress(student_id);
CREATE INDEX idx_fact_student_progress_course ON fact_student_progress(course_id);
CREATE INDEX idx_fact_student_progress_module ON fact_student_progress(module_id);
CREATE INDEX idx_fact_student_progress_lesson ON fact_student_progress(lesson_id);
CREATE INDEX idx_fact_student_progress_element ON fact_student_progress(element_id);
CREATE INDEX idx_fact_student_progress_time ON fact_student_progress(start_time);

-- Индексы для измерений
CREATE INDEX idx_student_registration ON dim_students(registration_date);
CREATE INDEX idx_course_creation ON dim_courses(creation_date);
CREATE INDEX idx_lesson_topic ON dim_lessons(topic_id);
CREATE INDEX idx_element_type ON dim_learning_elements(element_type_id);
CREATE INDEX idx_element_difficulty ON dim_learning_elements(difficulty_level);
CREATE INDEX idx_registration_date ON dim_course_registrations(registration_date);