-- stores per country
SELECT  country_code AS country,
        COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;


-- stores per locality
SELECT  locality,
        COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    total_no_stores DESC
LIMIT 
    7;


-- sales per month

SELECT  ROUND(SUM(dim_products.product_price * product_quantity)::NUMERIC, 2) AS total_sales,
        dim_date_times.month
FROM orders_table
JOIN
    dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY
    dim_date_times.month
ORDER BY 
    total_sales DESC
LIMIT
    6;


-- online sales

SELECT  COUNT(orders_table.product_code) AS number_of_sales,
        SUM(orders_table.product_quantity) AS product_quantity_count,
        dim_store_details.location
FROM
    orders_table
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    location
ORDER BY
    location DESC;


-- sales per store type

WITH cte AS (
SELECT  dim_store_details.store_type AS store_type,
        ROUND(SUM(dim_products.product_price * product_quantity)::NUMERIC, 2) AS total_sales
FROM orders_table
JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    dim_store_details.store_type
),
cte2 AS (
SELECT SUM(total_sales) AS all_sales_value
FROM 
    cte
)
SELECT  store_type,
        total_sales,
        ROUND(total_sales * 100 / all_sales_value, 2) AS percentage_total
FROM
    cte, cte2
GROUP BY
    store_type, total_sales, all_sales_value
ORDER BY 
    total_sales DESC;


-- best sales month per year

SELECT  ROUND(SUM(dim_products.product_price * product_quantity)::NUMERIC, 2) AS total_sales,
        dim_date_times.year,
        dim_date_times.month
FROM
    orders_table
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    dim_date_times.year, dim_date_times.month
ORDER BY
    total_sales DESC
LIMIT
    10;


-- staff per country

SELECT  SUM(staff_numbers) AS total_staff_numbers,
        country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;


-- german sales per store type

SELECT  ROUND(SUM(dim_products.product_price * product_quantity)::NUMERIC, 2) AS total_sales,
        store_type,
        country_code
FROM
    orders_table
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE
    country_code = 'DE'
GROUP BY
    store_type, country_code
ORDER BY
    total_sales ASC;


-- average time between sales

WITH cte AS (
SELECT  year,
        month,
        day,
        timestamp::time
FROM 
    dim_date_times
ORDER BY
    year ASC, month ASC, day ASC, timestamp ASC
),
cte2 AS (
SELECT  year,
        LEAD(timestamp, 1) OVER (
            PARTITION BY year
        ) AS time_difference
FROM
    cte
GROUP BY
    year, timestamp
)
SELECT year,
        time_difference
FROM cte2;

SELECT  year,
        AVG(time_difference) AS actual_time_taken
FROM
    cte2
GROUP BY
    year
ORDER BY
    actual_time_taken DESC;