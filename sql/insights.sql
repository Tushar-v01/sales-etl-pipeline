-- Query 1: Total revenue per month
SELECT
    year,
    month,
    COUNT(order_id)    AS total_orders,
    SUM(revenue)       AS total_revenue,
    AVG(revenue)       AS avg_order_value
FROM sales_clean
GROUP BY year, month
ORDER BY year, month;

-- Query 2: Running revenue total (window function)
SELECT
    month,
    SUM(revenue) AS monthly_revenue,
    SUM(SUM(revenue)) OVER (ORDER BY month) AS running_total
FROM sales_clean
GROUP BY month
ORDER BY month;

-- Query 3: Top product per month using RANK
WITH monthly_product AS (
    SELECT
        month,
        product,
        SUM(revenue) AS product_revenue,
        RANK() OVER (
            PARTITION BY month
            ORDER BY SUM(revenue) DESC
        ) AS rnk
    FROM sales_clean
    GROUP BY month, product
)
SELECT month, product, product_revenue
FROM monthly_product
WHERE rnk = 1;

-- Query 4: Top 5 cities by revenue
SELECT
    city,
    COUNT(order_id) AS orders,
    SUM(revenue)    AS total_revenue
FROM sales_clean
GROUP BY city
ORDER BY total_revenue DESC
LIMIT 5;