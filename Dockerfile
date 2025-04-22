# Используем официальный образ PostgreSQL
FROM postgres:15-alpine

# Установка переменных окружения
ENV POSTGRES_DB=online_courses
ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=secret

# Копируем SQL-скрипты для инициализации базы данных
COPY init.sql /docker-entrypoint-initdb.d/

# Открываем порт PostgreSQL
EXPOSE 5432

# Устанавливаем healthcheck для проверки работоспособности БД
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD pg_isready -U $POSTGRES_USER -d $POSTGRES_DB