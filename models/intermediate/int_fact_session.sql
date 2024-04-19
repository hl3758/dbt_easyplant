WITH PageViews AS (
    SELECT
        SESSION_ID,
        PAGE_NAME,
        VIEW_AT_TS,
        ROW_NUMBER() OVER (PARTITION BY SESSION_ID, PAGE_NAME ORDER BY VIEW_AT_TS) AS ROW_NUM
    FROM {{ ref('base_web_page_views') }}
)

, PageViewsTS AS (
    SELECT
        SESSION_ID,
        MAX(CASE WHEN PAGE_NAME = 'landing_page' THEN TRUE ELSE FALSE END) AS IF_VIEWED_LANDING_PAGE,
        MAX(CASE WHEN PAGE_NAME = 'landing_page' THEN VIEW_AT_TS ELSE NULL END) AS FIRST_LANDING_PAGE_VIEW_TS,
        MAX(CASE WHEN PAGE_NAME = 'faq' THEN TRUE ELSE FALSE END) AS IF_VIEWED_FAQ,
        MAX(CASE WHEN PAGE_NAME = 'faq' THEN VIEW_AT_TS ELSE NULL END) AS FIRST_FAQ_VIEW_TS,
        MAX(CASE WHEN PAGE_NAME = 'shop_plants' THEN TRUE ELSE FALSE END) AS IF_VIEWED_SHOP_PLANTS,
        MAX(CASE WHEN PAGE_NAME = 'shop_plants' THEN VIEW_AT_TS ELSE NULL END) AS FIRST_SHOP_PLANTS_VIEW_TS,
        MAX(CASE WHEN PAGE_NAME = 'cart' THEN TRUE ELSE FALSE END) AS IF_VIEWED_CART,
        MAX(CASE WHEN PAGE_NAME = 'cart' THEN VIEW_AT_TS ELSE NULL END) AS FIRST_CART_VIEW_TS,
        MAX(CASE WHEN PAGE_NAME = 'plant_care' THEN TRUE ELSE FALSE END) AS IF_VIEWED_PLANT_CARE,
        MAX(CASE WHEN PAGE_NAME = 'plant_care' THEN VIEW_AT_TS ELSE NULL END) AS FIRST_PLANT_CARE_VIEW_TS
    FROM PageViews
    WHERE ROW_NUM = 1
    GROUP BY SESSION_ID
)

, Orders AS (
    SELECT
        SESSION_ID,
        MIN(ORDER_AT_TS) AS FIRST_ORDER_TS
    FROM {{ ref('base_web_orders') }}
    GROUP BY SESSION_ID
)


SELECT
    S.SESSION_ID,
    CLIENT_ID,
    OS,
    IP,
    SESSION_AT_TS,
    IF_VIEWED_LANDING_PAGE,
    FIRST_LANDING_PAGE_VIEW_TS,
    IF_VIEWED_FAQ,
    FIRST_FAQ_VIEW_TS,
    IF_VIEWED_SHOP_PLANTS,
    FIRST_SHOP_PLANTS_VIEW_TS,
    IF_VIEWED_CART,
    FIRST_CART_VIEW_TS,
    IF_VIEWED_PLANT_CARE,
    FIRST_PLANT_CARE_VIEW_TS,
    CASE WHEN O.SESSION_ID IS NOT NULL THEN TRUE ELSE FALSE END AS IF_ORDERED,
    FIRST_ORDER_TS
FROM {{ ref('base_web_sessions') }} S
INNER JOIN PageViewsTS V ON S.SESSION_ID = V.SESSION_ID
LEFT JOIN Orders O ON S.SESSION_ID = O.SESSION_ID