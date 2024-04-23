select '40-55',
sum(case when REMOVE_QUANTITY_IN_LAST_WEEK < 50 then 1 else 0 end) as amount
from  {{ ref('dim_item') }} 
UNION ALL
select '55-70', 
sum(case when REMOVE_QUANTITY_IN_LAST_WEEK > 55 and REMOVE_QUANTITY_IN_LAST_WEEK < 70 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '70-85', 
sum(case when REMOVE_QUANTITY_IN_LAST_WEEK > 70 and REMOVE_QUANTITY_IN_LAST_WEEK < 85 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '85-99', 
sum(case when REMOVE_QUANTITY_IN_LAST_WEEK > 85 and REMOVE_QUANTITY_IN_LAST_WEEK < 99 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '99-115', 
sum(case when REMOVE_QUANTITY_IN_LAST_WEEK > 99 then 1 else 0 end)
from 
{{ ref('dim_item') }}