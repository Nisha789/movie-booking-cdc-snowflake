create database EComm;

Use Ecomm;

create or replace table raw_orders(
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP,
    status STRING,
    amount NUMBER,
    product_id STRING,
    quantity NUMBER
);

create or replace dynamic table orders_derived
warehouse = 'COMPUTE_WH'
TARGET_LAG = DOWNSTREAM
AS
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    amount,
    product_id,
    quantity,
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    CASE
        WHEN amount > 100 THEN 'High Value'
        ELSE 'Low Value'
    END AS order_value_category,
    CASE
        WHEN status = 'DELIVERED' THEN 'Completed'
        ELSE 'Pending'
    END AS order_completion_status,
    DATEDIFF('day',order_date, CURRENT_TIMESTAMP()) AS days_since_order
FROM raw_orders;

CREATE OR REPLACE DYNAMIC TABLE orders_aggregated
WAREHOUSE = 'COMPUTE_WH'
TARGET_LAG = DOWNSTREAM
AS
SELECT
    order_year,
    order_month,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as average_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) as total_items_sold,
    SUM(CASE WHEN status = 'DELIVERED' THEN amount ELSE 0 END) AS total_delivered_revenue,
    SUM(CASE WHEN order_value_category = 'High Value' THEN amount ELSE 0 END) as total_high_value_orders,
    SUM(CASE WHEN order_value_category = 'Low Value' Then amount ELSE 0 END) AS total_low_value_orders
FROM orders_derived
GROUP BY order_year,order_month;

select * FROM raw_orders;

select * from orders_derived;

select * from orders_aggregated;

CREATE OR REPLACE TASK refresh_orders_agg
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '2 MINUTE'
AS
ALTER DYNAMIC TABLE orders_aggregated REFRESH;

ALTER TASK refresh_orders_agg RESUME;

SELECT * FROM TABLE(INFORMATION_SCHEMA.task_history(task_name=>'refresh_orders_agg')) 
ORDER BY SCHEDULED_TIME;