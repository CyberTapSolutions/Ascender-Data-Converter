CREATE TABLE students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    grade_level INTEGER
);

CREATE TABLE staff (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100)
);

CREATE TABLE finance (
    id SERIAL PRIMARY KEY,
    category VARCHAR(100),
    amount DECIMAL(10, 2)
);
