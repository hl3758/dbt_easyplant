with a as (
select ITEM_NAME, count(*) as NUM_OF_ORDERS_IN_LAST_MONTH
from(
SELECT
    iv.ITEM_NAME,
    iv.SESSION_ID,
    o.ORDER_AT_TS,
    o.ORDER_ID
FROM
     {{ref('base_web_orders')}} as o
LEFT JOIN
    {{ref('base_web_item_views')}}  as iv
ON
    o.SESSION_ID=iv.SESSION_ID
)
WHERE
    ORDER_AT_TS  >= DATEADD(month, -1, CURRENT_DATE())
group by 1
),
b as
(
SELECT
    iv.ITEM_NAME,
    iv.PRICE_PER_UNIT as ITEM_PRICE,
    SUM(iv.ADD_TO_CART_QUANTITY) AS ADD_QUANTITY_IN_LAST_MONTH,
    SUM(iv.REMOVE_FROM_CART_QUANTITY) AS REMOVE_QUANTITY_IN_LAST_MONTH,
FROM
    {{ref('base_web_item_views')}}  as iv
WHERE
    iv.ITEM_VIEW_AT_TS >= DATEADD(month, -1, CURRENT_DATE()) 
GROUP BY
    iv.PRICE_PER_UNIT,iv.ITEM_NAME),

c as(
select ITEM_NAME, count(*) as returned_last_month, 
sum(case when is_refunded_boolean =TRUE then 1 else 0 end) as refunded_last_month,
round(refunded_last_month/returned_last_month,2) as refunded_returned_rate_last_month
from(
SELECT
    iv.ITEM_NAME,
    iv.SESSION_ID,
    o.order_id,
    r.ORDER_ID,
    r.IS_REFUNDED_BOOLEAN,
    r.RETURNED_AT_DATE
FROM
   {{ref('base_web_orders')}} as o
LEFT JOIN
    {{ref('base_web_item_views')}}  as iv
ON
    o.SESSION_ID=iv.SESSION_ID
inner join
    {{ref('base_google_drive_returns')}} as r 
on r.order_id=o.order_id
)
WHERE
    returned_at_date  >= DATEADD(month, -1, CURRENT_DATE())
group by item_name
)

select 
    b.item_name, 
    b.item_price, 
    b.add_quantity_in_last_month,
    b.remove_quantity_in_last_month,
    a.num_of_orders_in_last_month,
    c.returned_last_month,
    c.refunded_last_month,
    refunded_returned_rate_last_month
from a
join b
on a.item_name=b.item_name
join c
on a.item_name=c.item_name
order by 1