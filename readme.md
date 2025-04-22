
## Автор: Артём Шмараев
## Группа: БПИ-225

## ЗАДАЧА 1. (4 балла)


Я спроектировал схему хранения данных в виде схемы "звезда", где:
- Факт (fact_student_progress) содержит метрики и связи с измерениями.
- Измерения включают студентов, курсы, модули, уроки, темы, элементы и даты.
Это позволяет аналитикам делать запросы по любым комбинациям фильтров: время, сложности, темы, типы элементов и т.д.


## SQL: Схема данных (CREATE TABLE) ---
```sql
-- Создание таблицы типов элементов (самая независимая таблица)
CREATE TABLE dim_element_types (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255)
);

-- Создание таблицы тем
CREATE TABLE dim_topics (
    topic_id SERIAL PRIMARY KEY,
    topic_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    parent_topic_id INTEGER,
    CONSTRAINT fk_parent_topic FOREIGN KEY (parent_topic_id) REFERENCES dim_topics(topic_id)
);

-- Создание таблицы студентов
CREATE TABLE dim_students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    registration_date TIMESTAMP NOT NULL,
    last_login TIMESTAMP
);

-- Создание таблицы курсов
CREATE TABLE dim_courses (
    course_id SERIAL PRIMARY KEY,
    course_name VARCHAR(255) NOT NULL,
    description TEXT,
    creation_date TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

-- Создание таблицы модулей
CREATE TABLE dim_modules (
    module_id SERIAL PRIMARY KEY,
    course_id INTEGER NOT NULL,
    module_name VARCHAR(255) NOT NULL,
    module_order INTEGER NOT NULL,
    CONSTRAINT fk_course FOREIGN KEY (course_id) REFERENCES dim_courses(course_id)
);

-- Создание таблицы уроков
CREATE TABLE dim_lessons (
    lesson_id SERIAL PRIMARY KEY,
    module_id INTEGER NOT NULL,
    lesson_name VARCHAR(255) NOT NULL,
    lesson_order INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    CONSTRAINT fk_module FOREIGN KEY (module_id) REFERENCES dim_modules(module_id),
    CONSTRAINT fk_topic FOREIGN KEY (topic_id) REFERENCES dim_topics(topic_id)
);

-- Создание таблицы учебных элементов
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

-- Создание таблицы регистраций на курсы
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

-- Создание таблицы дат (для анализа)
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

-- Создание фактовой таблицы прогресса студентов (последней, так как она ссылается на все остальные)
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

-- Создание индексов для оптимизации запросов
CREATE INDEX idx_fact_student_progress_student ON fact_student_progress(student_id);
CREATE INDEX idx_fact_student_progress_course ON fact_student_progress(course_id);
CREATE INDEX idx_fact_student_progress_module ON fact_student_progress(module_id);
CREATE INDEX idx_fact_student_progress_lesson ON fact_student_progress(lesson_id);
CREATE INDEX idx_fact_student_progress_element ON fact_student_progress(element_id);
CREATE INDEX idx_fact_student_progress_time ON fact_student_progress(start_time);

CREATE INDEX idx_student_registration ON dim_students(registration_date);
CREATE INDEX idx_course_creation ON dim_courses(creation_date);
CREATE INDEX idx_lesson_topic ON dim_lessons(topic_id);
CREATE INDEX idx_element_type ON dim_learning_elements(element_type_id);
CREATE INDEX idx_element_difficulty ON dim_learning_elements(difficulty_level);
CREATE INDEX idx_registration_date ON dim_course_registrations(registration_date);

```


## ЗАДАЧА 2. (2 балла)
### Запрос 1:
```sql
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
```

### Запрос 2:
```sql
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
```

## ЗАДАЧА 3. (2 балла)

Спроектировал ключи Redis для хранения прогресса студентов.
Основная идея:
- прогресс в Hash progress:{student_id}:{element_id}
- completed элементы: Set completed:{student_id}
- обратный индекс: Set students_by_element:{element_id}
- вспомогательные ключи: элемент → урок, тема и т.д.


### Основная структура хранения

#### 1. **Прогресс по каждому элементу**  
**Тип: `HASH`**  
**Ключ:** `progress:{student_id}:{element_id}`  
**Пример:** `progress:42:101`

**Значения:**
```redis
course_id=10
module_id=2
lesson_id=5
topic_id=17
duration_seconds=1350
score=95
completed=true
```

---

#### 2. **Множество завершённых элементов у студента**  
**Тип: `SET`**  
**Ключ:** `completed:{student_id}`  
**Пример:** `completed:42`  
**Значения:** ID всех завершённых элементов (например, `101`, `102`, `103`)

---

#### 3. **Обратный индекс: какие студенты проходили элемент**  
**Тип: `SET`**  
**Ключ:** `students_by_element:{element_id}`  
**Пример:** `students_by_element:101`  
**Значения:** ID студентов, прошедших элемент (например, `42`, `73`, `108`)

---

#### 4. **Дата регистрации студента**  
**Тип: `STRING`**  
**Ключ:** `student_reg_date:{student_id}`  
**Пример:** `student_reg_date:42`  
**Значение:** `2024-03-12`

---

#### 5. **Метаданные об элементе**  
Для быстрого доступа без SQL.  
**Тип: `STRING`**  
Примеры ключей:
- `element_type:{element_id}` → `"video"`
- `element_topic:{element_id}` → `"Hadoop"`
- `element_difficulty:{element_id}` → `"4"`


# ЗАДАЧА 5. (2 балла)
- Найти все курсы про Hadoop со сложностью больше трех
```
# 1. Найти все элементы с topic = "Hadoop"

redis-cli --scan --pattern 'element_topic:*' | while read key; do
  topic=$(redis-cli GET "$key")
  if [ "$topic" = "Hadoop" ]; then
    element_id=${key#element_topic:}
    
    # 2. Проверить сложность элемента
    difficulty=$(redis-cli GET "element_difficulty:$element_id")
    if [ "$difficulty" -gt 3 ]; then
      
      # 3. Найти course_id для этого элемента
      student_id=$(redis-cli SMEMBERS "students_by_element:$element_id" | head -n 1)
      if [ -n "$student_id" ]; then
        course_id=$(redis-cli HGET "progress:$student_id:$element_id" course_id)
        echo "Курс $course_id (элемент $element_id, сложность $difficulty)"
      fi
    fi
  fi
done | sort -u  # Убираем дубликаты course_id
```

- Найти всех студентов, зарегистрированных в прошлом году
```
current_year=$(date +%Y)
last_year=$((current_year - 1))

# Сканируем все ключи регистраций
redis-cli --scan --pattern 'student_reg_date:*' | while read key; do
  reg_date=$(redis-cli GET "$key")
  if [[ "$reg_date" == "$last_year"* ]]; then
    student_id=${key#student_reg_date:}
    echo "$student_id"
  fi
done
```