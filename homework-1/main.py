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
            cursor.execute("TRUNCATE TABLE employees RESTART IDENTITY CASCADE;")

            with open('north_data/employees_data.csv', 'r', encoding='utf-8') as emp:
                emp_csv = csv.DictReader(emp)
                for data in emp_csv:
                    cursor.execute(
                        '''INSERT INTO employees (first_name, last_name, title, birth_date, notes)
                        VALUES (%s, %s, %s, %s, %s) ''', (data['first_name'],
                                                          data['last_name'],
                                                          data['title'],
                                                          data['birth_date'],
                                                          data['notes']
                                                          )
                    )

            cursor.execute('''TRUNCATE TABLE customers RESTART IDENTITY CASCADE;''')

            with open(file='north_data/customers_data.csv', mode='r', encoding='utf-8') as cust:
                cust_csv = csv.DictReader(cust)
                for data_cust in cust_csv:
                    cursor.execute('''INSERT INTO customers ("customer_id", "company_name", "contact_name")
                    VALUES (%s, %s, %s)''', (data_cust["customer_id"],
                                            data_cust["company_name"],
                                            data_cust["contact_name"]
                                            )
                                   )


            cursor.execute('SELECT * FROM customers;')
            print(cursor.fetchall())


except Exception as ex:
    print(f'Что то не так {ex}')
