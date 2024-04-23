SELECT 'Total_all' AS source, 'Total_profit' AS target, SUM(DAILY_PROFIT) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'Total_all' AS source, 'Total_expense' AS target, SUM(DAILY_EXPENSE + DAILY_SALARY + DAILY_SHIPPING_COST + DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'Total_profit' AS source, 'Total_profit' AS target, SUM(DAILY_REVENUE - DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'Total_expense' AS source, 'Total_SHIPPING_COST)' AS target, SUM(DAILY_SHIPPING_COST) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'Total_expense' AS source, 'Total_DAILY_TAX' AS target, SUM(DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'Total_expense' AS source, 'Total_DAILY_SALARY' AS target, SUM(DAILY_SALARY) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'Total_expense' AS source, 'Total_DAILY_EXPENSE' AS target, SUM(DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}