SELECT 'ADD_QUANTITY', SUM(ADD_QUANTITY_IN_LAST_WEEK) AS amount FROM {{ ref('dim_item') }}
UNION ALL
SELECT 'RETAINED_QUANTITY', SUM(ADD_QUANTITY_IN_LAST_WEEK)-sum(REMOVE_QUANTITY_IN_LAST_WEEK) FROM {{ ref('dim_item') }}
UNION ALL
SELECT 'ORDER_QUANTITY', SUM(NUM_OF_ORDERS_IN_LAST_WEEK) FROM {{ ref('dim_item') }}
UNION ALL
SELECT 'RETURNED_QUANTITY', SUM(RETURNED_LAST_WEEK) FROM {{ ref('dim_item') }}
UNION ALL
SELECT 'REFUNDED_QUANTITY', SUM(REFUNDED_LAST_WEEK) FROM {{ ref('dim_item') }}