CREATE DATABASE IF NOT EXISTS shop;

CREATE TABLE shop.users (
    id Int32,
    name String,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE shop.orders (
    id Int32,
    user_id Int32,
    amount Decimal(10, 2),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY id;
