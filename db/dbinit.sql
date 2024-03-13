-- Check if the database "dev" exists
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dev-db') THEN
        -- Create the database if it doesn't exist
        CREATE DATABASE dev;
    END IF;
END $$;

-- Connect to the "dev" database
\c dev-db;

-- Create a table to store user data if it doesn't exist
CREATE TABLE IF NOT EXISTS users (
    id serial PRIMARY KEY,
    name varchar(255),
    email varchar(255),
    password varchar(255)
);

ALTER TABLE users REPLICA IDENTITY FULL;
-- Check if the "users" table is empty
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM users) THEN
        -- Insert example data into the "users" table
        INSERT INTO users (name, email, password) VALUES
            ('John Doe', 'john.doe@example.com', 'password123'),
            ('Jane Doe', 'jane.doe@example.com', 'password456');
    END IF;
END $$;

-- Create a table to store order data if it doesn't exist
CREATE TABLE IF NOT EXISTS orders (
    id serial PRIMARY KEY,
    user_id int REFERENCES users(id),
    product_id int,
    quantity int,
    created_at timestamp
);

ALTER TABLE orders REPLICA IDENTITY FULL;
-- Check if the "orders" table is empty
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM orders) THEN
        -- Insert example data into the "orders" table
        INSERT INTO orders (user_id, product_id, quantity, created_at) VALUES
            (1, 100, 1, NOW()),
            (1, 200, 2, NOW()),
            (2, 300, 3, NOW());
    END IF;
END $$;



CREATE TABLE IF NOT EXISTS products (
  product_id SERIAL PRIMARY KEY,
  product_name VARCHAR(255) NOT NULL,
  quantity_in INTEGER NOT NULL,
  cost_price NUMERIC(10,2) NOT NULL,
  quantity_sold INTEGER NOT NULL,
  selling_price NUMERIC(10,2) NOT NULL,
  discounted_price NUMERIC(10,2) NOT NULL,
  status VARCHAR(255) NOT NULL,
  category VARCHAR(255) NOT NULL,
  unit VARCHAR(255) NOT NULL,
  market_id INTEGER NOT NULL
);

ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS markets (
  market_id SERIAL PRIMARY KEY,
  capacity INTEGER NOT NULL,
  current_inventory INTEGER NOT NULL,
  market_name VARCHAR(255) NOT NULL,
  address VARCHAR(255) NOT NULL
);

ALTER TABLE markets REPLICA IDENTITY FULL;