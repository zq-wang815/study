create database if not exists spark_offline_demo default character set utf8mb4;
use spark_offline_demo;

drop table if exists payment_info;
drop table if exists order_item;
drop table if exists order_info;
drop table if exists product_info;
drop table if exists shop_info;
drop table if exists user_info;

create table user_info (
    id bigint primary key,
    user_name varchar(64),
    user_level varchar(16),
    province varchar(32),
    register_time datetime
);

create table shop_info (
    id bigint primary key,
    shop_name varchar(64),
    shop_level varchar(16),
    city varchar(32)
);

create table product_info (
    id bigint primary key,
    product_name varchar(128),
    category_name varchar(64),
    shop_id bigint,
    origin_price decimal(10,2)
);

create table order_info (
    id bigint primary key,
    user_id bigint,
    order_status varchar(16),
    total_amount decimal(10,2),
    pay_amount decimal(10,2),
    order_time datetime,
    pay_time datetime
);

create table order_item (
    id bigint primary key,
    order_id bigint,
    product_id bigint,
    quantity int,
    sale_price decimal(10,2),
    final_price decimal(10,2),
    coupon_amount decimal(10,2)
);

create table payment_info (
    id bigint primary key,
    order_id bigint,
    payment_type varchar(16),
    payment_status varchar(16),
    payment_amount decimal(10,2),
    callback_time datetime
);

insert into user_info values
(1001, 'Alice', 'VIP', 'Shanghai', '2025-11-01 10:00:00'),
(1002, 'Bob', 'NORMAL', 'Beijing', '2025-12-08 15:30:00'),
(1003, 'Carol', 'SVIP', 'Hangzhou', '2026-01-18 09:45:00'),
(1004, 'David', 'NORMAL', 'Shenzhen', '2026-03-03 20:10:00');

insert into shop_info values
(2001, 'Fresh Mart', 'A', 'Shanghai'),
(2002, 'Digital Hub', 'A', 'Shenzhen'),
(2003, 'Home Choice', 'B', 'Suzhou');

insert into product_info values
(3001, 'Organic Milk', 'Food', 2001, 56.00),
(3002, 'Wireless Earbuds', 'Electronics', 2002, 199.00),
(3003, 'Desk Lamp', 'Home', 2003, 89.00),
(3004, 'Coffee Beans', 'Food', 2001, 79.00);

insert into order_info values
(5001, 1001, 'PAID', 311.00, 291.00, '2026-05-14 09:10:00', '2026-05-14 09:12:00'),
(5002, 1002, 'CREATED', 199.00, 0.00, '2026-05-14 10:20:00', null),
(5003, 1003, 'PAID', 267.00, 267.00, '2026-05-14 11:05:00', '2026-05-14 11:08:00'),
(5004, 1004, 'PAID', 158.00, 149.00, '2026-05-14 13:30:00', '2026-05-14 13:35:00');

insert into order_item values
(6001, 5001, 3001, 2, 56.00, 51.00, 10.00),
(6002, 5001, 3004, 3, 79.00, 63.00, 18.00),
(6003, 5002, 3002, 1, 199.00, 199.00, 0.00),
(6004, 5003, 3003, 3, 89.00, 89.00, 0.00),
(6005, 5004, 3001, 1, 56.00, 50.00, 6.00),
(6006, 5004, 3002, 1, 199.00, 99.00, 100.00);

insert into payment_info values
(7001, 5001, 'ALIPAY', 'SUCCESS', 291.00, '2026-05-14 09:12:00'),
(7002, 5002, 'WECHAT', 'INIT', 0.00, null),
(7003, 5003, 'ALIPAY', 'SUCCESS', 267.00, '2026-05-14 11:08:00'),
(7004, 5004, 'WECHAT', 'SUCCESS', 149.00, '2026-05-14 13:35:00');
