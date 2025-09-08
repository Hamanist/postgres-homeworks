-- SQL-команды для создания таблиц
-- 1. Таблица сотрудников
CREATE TABLE employees (
	employee_id SERIAL PRIMARY KEY,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	title TEXT,
	birth_date DATE,
	notes TEXT
);

-- 2. Таблица клиентов
CREATE TABLE customers (
	customer_id VARCHAR(10) PRIMARY KEY,
	company_name VARCHAR(100) NOT NULL,
	contact_name VARCHAR(50) NOT NULL
);

-- 3. Таблица заказов
CREATE TABLE orders (
	order_id INT PRIMARY KEY,
	customer_id VARCHAR(10) NOT NULL,
	employee_id INT NOT NULL,
	order_date DATE NOT NULL,
	ship_city VARCHAR(50) NOT NULL,
	FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
	FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);