SELECT
    ss.CLIENT_ID,
    oo.CLIENT_NAME,
    oo.PHONE,
    oo.STATE,
    oo.SHIPPING_ADDRESS,
    ss.OS,
    ss.IP

    
FROM {{ ref('base_web_sessions') }} AS ss
LEFT JOIN {{ ref('base_web_orders') }} AS oo
ON ss.SESSION_ID = oo.SESSION_ID
where CLIENT_NAME IS NOT NULL