SELECT
    ss.CLIENT_ID,
    ROW_NUMBER() OVER (PARTITION BY ss.CLIENT_ID ORDER BY oo.CLIENT_NAME asc) as ROW_NUM,
    oo.CLIENT_NAME,
    oo.PHONE,
    oo.STATE,
    oo.SHIPPING_ADDRESS,
    ss.OS,
    ss.IP
    
    
FROM {{ ref('base_web_sessions') }} AS ss
INNER JOIN {{ ref('base_web_orders') }} AS oo
ON ss.SESSION_ID = oo.SESSION_ID
where CLIENT_NAME IS NOT NULL
order by 1, 3