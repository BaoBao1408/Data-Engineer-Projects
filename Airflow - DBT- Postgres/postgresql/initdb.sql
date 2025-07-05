-- Tabel categories
CREATE TABLE categories (
    "id" INT,
    "name" VARCHAR,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP
);

-- Load init data categories table
COPY categories(id, name, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/categories.csv'
DELIMITER ','
CSV HEADER;

-- Tabel goods
CREATE TABLE goods (
    "id" INT,
    "category_id" INT,
    "name" VARCHAR,
    "price" FLOAT,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP
);

-- Load init data goods table
COPY goods(id, category_id, name, price, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/goods.csv'
DELIMITER ','
CSV HEADER;

-- Tabel orders
CREATE TABLE orders (
    "id" INT,
    "user_id" INT,
    "is_refund" BOOLEAN,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP
);

-- Load init data into orders table
COPY orders(id, user_id, is_refund, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/orders.csv'
DELIMITER ','
CSV HEADER;

-- Tabel order_item
CREATE TABLE order_item (
    "id" INT,
    "order_id" INT,
    "goods_id" INT,
    "qty" INT,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP
);

-- Load init data into order_item table
COPY order_item(id, order_id, goods_id, qty, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/order_item.csv'
DELIMITER ','
CSV HEADER;

-- Tabel users
CREATE TABLE users (
    "id" INT,
    "name" VARCHAR,
    "email" VARCHAR,
    "phone" VARCHAR,
    "created_at" TIMESTAMP,
    "updated_at" TIMESTAMP
);

-- Load init data into users table
COPY users(id, name, email, phone, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/users.csv'
DELIMITER ','
CSV HEADER;
