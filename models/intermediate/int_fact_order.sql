WITH ORDER_ITEM AS (
    SELECT
        orders.ORDER_ID,
        orders.ORDER_AT_TS,
        PAYMENT_INFO,
        PAYMENT_METHOD,
        PHONE,
        SHIPPING_ADDRESS,
        STATE,
        CASE
           WHEN state = 'Alabama' THEN 'US-AL'
           WHEN state = 'Alaska' THEN 'US-AK'
           WHEN state = 'Arizona' THEN 'US-AZ'
           WHEN state = 'Arkansas' THEN 'US-AR'
           WHEN state = 'California' THEN 'US-CA'
           WHEN state = 'Colorado' THEN 'US-CO'
           WHEN state = 'Connecticut' THEN 'US-CT'
           WHEN state = 'Delaware' THEN 'US-DE'
           WHEN state = 'Florida' THEN 'US-FL'
           WHEN state = 'Georgia' THEN 'US-GA'
           WHEN state = 'Hawaii' THEN 'US-HI'
           WHEN state = 'Idaho' THEN 'US-ID'
           WHEN state = 'Illinois' THEN 'US-IL'
           WHEN state = 'Indiana' THEN 'US-IN'
           WHEN state = 'Iowa' THEN 'US-IA'
           WHEN state = 'Kansas' THEN 'US-KS'
           WHEN state = 'Kentucky' THEN 'US-KY'
           WHEN state = 'Louisiana' THEN 'US-LA'
           WHEN state = 'Maine' THEN 'US-ME'
           WHEN state = 'Maryland' THEN 'US-MD'
           WHEN state = 'Massachusetts' THEN 'US-MA'
           WHEN state = 'Michigan' THEN 'US-MI'
           WHEN state = 'Minnesota' THEN 'US-MN'
           WHEN state = 'Mississippi' THEN 'US-MS'
           WHEN state = 'Missouri' THEN 'US-MO'
           WHEN state = 'Montana' THEN 'US-MT'
           WHEN state = 'Nebraska' THEN 'US-NE'
           WHEN state = 'Nevada' THEN 'US-NV'
           WHEN state = 'New Hampshire' THEN 'US-NH'
           WHEN state = 'New Jersey' THEN 'US-NJ'
           WHEN state = 'New Mexico' THEN 'US-NM'
           WHEN state = 'New York' THEN 'US-NY'
           WHEN state = 'North Carolina' THEN 'US-NC'
           WHEN state = 'North Dakota' THEN 'US-ND'
           WHEN state = 'Ohio' THEN 'US-OH'
           WHEN state = 'Oklahoma' THEN 'US-OK'
           WHEN state = 'Oregon' THEN 'US-OR'
           WHEN state = 'Pennsylvania' THEN 'US-PA'
           WHEN state = 'Rhode Island' THEN 'US-RI'
           WHEN state = 'South Carolina' THEN 'US-SC'
           WHEN state = 'South Dakota' THEN 'US-SD'
           WHEN state = 'Tennessee' THEN 'US-TN'
           WHEN state = 'Texas' THEN 'US-TX'
           WHEN state = 'Utah' THEN 'US-UT'
           WHEN state = 'Vermont' THEN 'US-VT'
           WHEN state = 'Virginia' THEN 'US-VA'
           WHEN state = 'Washington' THEN 'US-WA'
           WHEN state = 'West Virginia' THEN 'US-WV'
           WHEN state = 'Wisconsin' THEN 'US-WI'
           WHEN state = 'Wyoming' THEN 'US-WY'
           ELSE NULL
        END AS STATE_CODE,
        sessions.SESSION_ID,
        SHIPPING_COST,
        TAX_RATE,
        CASE WHEN RETURNS.ORDER_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IS_RETURNED_BOOLEAN,
        CASE WHEN RETURNS.IS_REFUNDED_BOOLEAN IS NULL THEN FALSE ELSE RETURNS.IS_REFUNDED_BOOLEAN END AS IS_REFUNDED_BOOLEAN,
        RETURNED_AT_DATE,
        ITEM_NAME,
        SUM(ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY) AS TOTAL_IN_CART_QUANTITY,
        -- OBJECT_AGG(ITEM_NAME, (ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY)) AS ITEMS,
        SUM((ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY) * PRICE_PER_UNIT) AS ITEM_COST,
        SUM(((ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY) * PRICE_PER_UNIT + SHIPPING_COST) * (TAX_RATE)) AS TAX_COST, 
        SUM(((ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY) * PRICE_PER_UNIT + SHIPPING_COST) * (1 + TAX_RATE)) AS TOTAL_COST
    FROM {{ ref('base_web_orders') }} AS orders
    LEFT JOIN {{ ref('base_google_drive_returns') }} as returns
    ON orders.ORDER_ID = returns.ORDER_ID
    JOIN {{ ref('base_web_sessions') }} as sessions
    ON orders.SESSION_ID = sessions.SESSION_ID
    JOIN {{ ref('base_web_item_views') }} as item_views
    ON orders.SESSION_ID = item_views.SESSION_ID
    WHERE (ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY) > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
)

SELECT
    ORDER_ID,
    ORDER_AT_TS,
    PAYMENT_INFO,
    PAYMENT_METHOD,
    PHONE,
    SHIPPING_ADDRESS,
    STATE,
    STATE_CODE,
    SESSION_ID,
    SHIPPING_COST,
    TAX_RATE,
    IS_RETURNED_BOOLEAN,
    IS_REFUNDED_BOOLEAN,
    RETURNED_AT_DATE,
    OBJECT_AGG(ITEM_NAME, TOTAL_IN_CART_QUANTITY) AS ITEMS,
    SUM(ITEM_COST) AS ITEM_COST,
    SUM(TAX_COST) AS TAX_COST, 
    SUM(TOTAL_COST) AS TOTAL_COST
FROM ORDER_ITEM
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14