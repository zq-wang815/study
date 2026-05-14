CREATE DATABASE IF NOT EXISTS test DEFAULT CHARACTER SET utf8mb4;
USE test;

DROP TABLE IF EXISTS rt_user_info;
CREATE TABLE rt_user_info (
    user_id BIGINT NOT NULL,
    user_name VARCHAR(64) NOT NULL,
    user_level VARCHAR(32) NOT NULL,
    city VARCHAR(64) NOT NULL,
    register_time DATETIME NOT NULL,
    update_time DATETIME NOT NULL,
    PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS rt_shop_info;
CREATE TABLE rt_shop_info (
    shop_id BIGINT NOT NULL,
    shop_name VARCHAR(64) NOT NULL,
    shop_level VARCHAR(32) NOT NULL,
    city VARCHAR(64) NOT NULL,
    update_time DATETIME NOT NULL,
    PRIMARY KEY (shop_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS rt_product_info;
CREATE TABLE rt_product_info (
    product_id BIGINT NOT NULL,
    product_name VARCHAR(64) NOT NULL,
    category VARCHAR(32) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    shop_id BIGINT NOT NULL,
    update_time DATETIME NOT NULL,
    PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO rt_user_info (user_id, user_name, user_level, city, register_time, update_time) VALUES
(1001, 'Alice', 'VIP', 'Shanghai', '2026-05-14 10:00:00', '2026-05-14 10:00:00'),
(1002, 'Bob', 'NORMAL', 'Beijing', '2026-05-14 10:01:00', '2026-05-14 10:01:00'),
(1003, 'Carol', 'VIP', 'Hangzhou', '2026-05-14 10:02:00', '2026-05-14 10:02:00');

INSERT INTO rt_shop_info (shop_id, shop_name, shop_level, city, update_time) VALUES
(2001, 'Fresh Mart', 'A', 'Shanghai', '2026-05-14 10:00:00'),
(2002, 'Digital Hub', 'A', 'Shenzhen', '2026-05-14 10:00:00'),
(2003, 'Home Choice', 'B', 'Suzhou', '2026-05-14 10:00:00');

INSERT INTO rt_product_info (product_id, product_name, category, price, shop_id, update_time) VALUES
(3001, 'Organic Milk', 'Food', 12.50, 2001, '2026-05-14 10:00:00'),
(3002, 'Wireless Earbuds', 'Electronics', 199.00, 2002, '2026-05-14 10:00:00'),
(3003, 'Desk Lamp', 'Home', 89.00, 2003, '2026-05-14 10:00:00');
