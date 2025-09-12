"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
from dotenv import load_dotenv
import os
import psycopg2


load_dotenv()
try:
    with psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT version();'
            )
            print(f'СЕРВЕР {cursor.fetchone()}')


except Exception as ex:
    print(f'Что то не так {ex}')
finally:
    if not connection:
        connection.close()