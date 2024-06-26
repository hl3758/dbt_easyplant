-- SELECT 'DAILY_EXPENSE', SUM(DAILY_EXPENSE) FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'DAILY_PROFIT', SUM(DAILY_PROFIT) FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'DAILY_REVENUE', SUM(DAILY_REVENUE) FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'DAILY_SALARY', SUM(DAILY_SALARY) FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'DAILY_SHIPPING_COST', SUM(DAILY_SHIPPING_COST) FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'DAILY_TAX', SUM(DAILY_TAX) FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES




-- SELECT 'total_all' AS source, 'total_profit' AS target, SUM(DAILY_PROFIT) AS sum
-- FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'total_all' AS source, 'total_expense' AS target, SUM(DAILY_EXPENSE + DAILY_SALARY + DAILY_SHIPPING_COST + DAILY_TAX) AS sum
-- FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'total_profit' AS source, 'total_profit' AS target, SUM(DAILY_REVENUE - DAILY_EXPENSE) AS sum
-- FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'total_expense' AS source, 'sum(DAILY_SHIPPING_COST)' AS target, SUM(DAILY_SHIPPING_COST) AS sum
-- FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'total_expense' AS source, 'sum(DAILY_TAX)' AS target, SUM(DAILY_TAX) AS sum
-- FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'total_expense' AS source, 'sum(DAILY_SALARY)' AS target, SUM(DAILY_SALARY) AS sum
-- FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES
-- UNION ALL
-- SELECT 'total_expense' AS source, 'sum(DAILY_EXPENSE)' AS target, SUM(DAILY_EXPENSE) AS sum
-- FROM PROD.DEFAULT_ANALYTICS.DAILY_FINANCES


SELECT 
    DATE AS date,
    'TOTAL_ALL' AS source, 
    'TOTAL_PROFIT' AS target, 
    SUM(DAILY_PROFIT) AS sum
FROM {{ ref('daily_finances') }}
GROUP BY 
    DATE

UNION ALL

SELECT 
    DATE AS date,
    'TOTAL_ALL' AS source, 
    'TOTAL_EXPENSE' AS target, 
    SUM(DAILY_EXPENSE + DAILY_SALARY + DAILY_SHIPPING_COST + DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
GROUP BY 
    DATE

UNION ALL

SELECT 
    DATE AS date,
    'TOTAL_PROFIT' AS source, 
    'PROFIT' AS target, 
    SUM(DAILY_REVENUE - DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}
GROUP BY 
    DATE

UNION ALL

SELECT 
    DATE AS date,
    'TOTAL_EXPENSE' AS source, 
    'TOTAL_SHIPPING_COST' AS target, 
    SUM(DAILY_SHIPPING_COST) AS sum
FROM {{ ref('daily_finances') }}
GROUP BY 
    DATE

UNION ALL

SELECT 
    DATE AS date,
    'TOTAL_EXPENSE' AS source, 
    'TOTAL_DAILY_TAX' AS target, 
    SUM(DAILY_TAX) AS sum
FROM {{ ref('daily_finances') }}
GROUP BY 
    DATE

UNION ALL

SELECT 
    DATE AS date,
    'TOTAL_EXPENSE' AS source, 
    'TOTAL_DAILY_SALARY' AS target, 
    SUM(DAILY_SALARY) AS sum
FROM {{ ref('daily_finances') }}
GROUP BY 
    DATE

UNION ALL

SELECT 
    DATE AS date,
    'TOTAL_EXPENSE' AS source, 
    'TOTAL_DAILY_EXPENSE' AS target, 
    SUM(DAILY_EXPENSE) AS sum
FROM {{ ref('daily_finances') }}
GROUP BY 
    DATE
