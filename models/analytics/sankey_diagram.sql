SELECT 'TOTAL_ALL' AS source, 'TOTAL_PROFIT' AS target, SUM(DAILY_PROFIT) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'TOTAL_ALL' AS source, 'TOTAL_EXPENSE' AS target, SUM(DAILY_EXPENSE + DAILY_SALARY + DAILY_SHIPPING_COST + DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'TOTAL_PROFIT' AS source, 'PROFIT' AS target, SUM(DAILY_REVENUE - DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'TOTAL_EXPENSE' AS source, 'TOTAL_SHIPPING_COST' AS target, SUM(DAILY_SHIPPING_COST) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'TOTAL_EXPENSE' AS source, 'TOTAL_DAILY_TAX' AS target, SUM(DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'TOTAL_EXPENSE' AS source, 'TOTAL_DAILY_SALARY' AS target, SUM(DAILY_SALARY) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'TOTAL_EXPENSE' AS source, 'TOTAL_DAILY_EXPENSE' AS target, SUM(DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}