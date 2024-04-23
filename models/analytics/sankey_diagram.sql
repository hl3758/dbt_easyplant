SELECT 'total_all' AS source, 'total_profit' AS target, SUM(DAILY_PROFIT) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'total_all' AS source, 'total_expense' AS target, SUM(DAILY_EXPENSE + DAILY_SALARY + DAILY_SHIPPING_COST + DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'total_profit' AS source, 'total_profit' AS target, SUM(DAILY_REVENUE - DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'total_expense' AS source, 'sum(DAILY_SHIPPING_COST)' AS target, SUM(DAILY_SHIPPING_COST) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'total_expense' AS source, 'sum(DAILY_TAX)' AS target, SUM(DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'total_expense' AS source, 'sum(DAILY_SALARY)' AS target, SUM(DAILY_SALARY) AS sum
FROM {{ ref('daily_finances') }}
UNION ALL
SELECT 'total_expense' AS source, 'sum(DAILY_EXPENSE)' AS target, SUM(DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}