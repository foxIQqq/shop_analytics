CREATE TABLE IF NOT EXISTS customers (
    customer_id    SERIAL PRIMARY KEY,
    first_name     VARCHAR(100) NOT NULL,
    last_name      VARCHAR(100) NOT NULL,
    email          VARCHAR(255) UNIQUE NOT NULL,
    phone          VARCHAR(20),
    created_at     TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

INSERT INTO customers (first_name, last_name, email, phone, created_at)
VALUES
  ('Ivan', 'Petrov', 'ivan.petrov@example.com','+7-999-000-11-22','2024-12-05'),
  ('Anna', 'Ivanova', 'anna.ivanova@example.com','+7-999-000-33-44','2025-03-10'),
  ('Petr', 'Sidorov', 'petr.sidorov@example.com','+7-999-000-55-66','2025-05-20');