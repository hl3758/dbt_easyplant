SELECT 
    orders.ORDER_ID,
    SHIPPING_ADDRESS,
    SHIPPING_COST,
    PAYMENT_INFO,
    STATE,
    SESSION_ID,
    CLIENT_NAME,
    PHONE,
    TAX_RATE,
    PAYMENT_METHOD,
    IS_REFUNDED_BOOLEAN,
FROM {{ ref('base_web_orders') }} as orders
LEFT JOIN {{ ref('base_google_drive_returns') }} AS returns
ON orders.ORDER_ID = returns.ORDER_ID