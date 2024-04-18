WITH SalesData AS (
    SELECT
        s.sell_date,
        round(SUM(e.daily_salary), 2) AS total_daily_salaries,
        round(SUM(s.daily_profit),2) AS total_daily_profit,
        SUM(s.total_shipping_cost) AS total_shipping_cost,
        round(SUM(s.total_tax_amount),2) AS total_tax_amount
    FROM
        (
            SELECT
                iv.sell_date,
                SUM(iv.daily_profit) AS daily_profit,
                SUM(o.SHIPPING_COST) AS total_shipping_cost,
                SUM((o.SHIPPING_COST + iv.daily_profit) * o.TAX_RATE) AS total_tax_amount
            FROM
                (
                    SELECT
                        o.SESSION_ID,
                        o.ORDER_AT_TS,
                        CAST(TRIM(REPLACE(o.SHIPPING_COST, 'USD', '')) AS FLOAT) AS SHIPPING_COST,
                        o.TAX_RATE,
                        CAST(r.ORDER_ID AS STRING) AS ORDER_ID,
                        CASE WHEN r.IS_REFUNDED_BOOLEAN = 'yes' THEN TRUE WHEN r.IS_REFUNDED_BOOLEAN = 'no' THEN FALSE ELSE NULL END AS IS_REFUNDED_BOOLEAN
                    FROM
                        {{ref('base_google_drive_returns')}} AS r
                    INNER JOIN
                        {{ref('base_web_orders')}} AS o
                    ON
                        CAST(r.ORDER_ID AS STRING) = o.ORDER_ID
                    WHERE
                        r.IS_REFUNDED_BOOLEAN = 'no'
                ) AS o
            INNER JOIN
                (
                    SELECT
                        SESSION_ID,
                        TO_DATE(DATE_TRUNC('day', ITEM_VIEW_AT_TS)) AS sell_date,
                        SUM((ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY) * PRICE_PER_UNIT) AS daily_profit
                    FROM
                        {{ref('base_web_item_views')}}
                    WHERE
                        ADD_TO_CART_QUANTITY != 0
                    GROUP BY
                        SESSION_ID, TO_DATE(DATE_TRUNC('day', ITEM_VIEW_AT_TS))
                ) AS iv
            ON
                o.SESSION_ID = iv.SESSION_ID
            GROUP BY
                iv.sell_date
        ) AS s
    INNER JOIN
        (
            SELECT 
                r.EMPLOYEE_ID,
                CAST(TRIM(REPLACE(r.EMPLOYEE_HIRE_DATE, 'day', '')) AS DATE) AS EMPLOYEE_HIRE_DATE,
                r.EMPLOYEE_ANNUAL_SALARY AS EMPLOYEE_ANNUAL_SALARY,
                q.EMPLOYEE_QUIT_DATE AS EMPLOYEE_QUIT_DATE,
                (r.EMPLOYEE_ANNUAL_SALARY / 365) AS daily_salary
            FROM 
                {{ref('base_GOOGLE_DRIVE_requests_JOINS')}} AS r
            INNER JOIN 
                {{ref('base_GOOGLE_DRIVE_requests_QUITS')}} AS q
            ON 
                r.EMPLOYEE_ID = q.EMPLOYEE_ID
        ) AS e
    ON
        s.sell_date >= e.EMPLOYEE_HIRE_DATE
        AND (s.sell_date <= e.EMPLOYEE_QUIT_DATE OR e.EMPLOYEE_QUIT_DATE IS NULL)
    GROUP BY
        s.sell_date
),
ExpenseData AS (
    SELECT 
        EXPENSES_DATE AS EXPENSES_DATE,
        SUM(CAST(TRIM(REPLACE(EXPENSE_AMOUNT, '$', '')) AS FLOAT)) AS TOTAL_EXPENSE_AMOUNT
    FROM 
        {{ref('base_google_drive_expenses')}}
    GROUP BY 
        EXPENSES_DATE
)
SELECT 
    sd.sell_date AS "DATE",
    sd.total_daily_salaries AS "DAILY_SALARIES",
    sd.total_daily_profit AS "DAILY_PROFIT",
    sd.total_shipping_cost AS "DAILY_SHIPPING_COST",
    sd.total_tax_amount AS "DAILY_TAX",
    ed.TOTAL_EXPENSE_AMOUNT AS "DAILY_EXPENSE"
FROM 
    SalesData sd
LEFT JOIN 
    ExpenseData ed
ON 
    sd.sell_date = ed.EXPENSES_DATE
ORDER BY 
    sd.sell_date