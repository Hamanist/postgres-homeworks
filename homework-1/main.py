"""Скрипт для заполнения данными таблиц в БД Postgres."""
import os
from dotenv import load_dotenv
import psycopg2
import csv

load_dotenv()

with psycopg2.connect(
        host=os.getenv('HOST'),
        database=os.getenv('DATABASE'),
        user=os.getenv('USER_P'),
        password=os.getenv('PASSWORD')) as conn:
    with conn.cursor() as curs:
        with open('north_data/employees_data.csv', mode='r', encoding='utf-8') as file:
            file_employees = csv.reader(file)
            next(file_employees)
            for data_csv in file_employees:
                curs.execute(
                    'INSERT INTO employees (first_name, last_name, title, birth_date, notes)'
                    ' VALUES (%s, %s, %s, %s, %s)',
                    (data_csv[1], data_csv[2], data_csv[3], data_csv[4], data_csv[5]))

        with open('north_data/customers_data.csv', mode='r', encoding='utf-8') as file:
            file_customers = csv.DictReader(file)

            for data_csv in file_customers:
                curs.execute(
                    'INSERT INTO customers (customer_id, company_name, contact_name)'
                    'VALUES (%s, %s, %s)',
                    (data_csv['customer_id'], data_csv['company_name'], data_csv['contact_name'])
                )

        with open('north_data/orders_data.csv', mode='r', encoding='utf-8') as file:
            file_orders = csv.DictReader(file)

            for data_csv in file_orders:
                curs.execute(
                    'INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)'
                    'VALUES (%s, %s, %s, %s, %s)',
                    (data_csv['order_id'],
                     data_csv['customer_id'],
                     data_csv['employee_id'],
                     data_csv['order_date'],
                     data_csv['ship_city'])
                )

conn.close()
