select '600-650',
sum(case when ADD_QUANTITY_IN_LAST_WEEK < 650 then 1 else 0 end) as amount
from  {{ ref('dim_item') }}
UNION ALL
select '650-700', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 650 and ADD_QUANTITY_IN_LAST_WEEK < 700 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '700-750', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 700 and ADD_QUANTITY_IN_LAST_WEEK < 750 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '750-800', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 750 and ADD_QUANTITY_IN_LAST_WEEK < 800 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '850-850', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 800 and ADD_QUANTITY_IN_LAST_WEEK < 850 then 1 else 0 end)
from 
{{ ref('dim_item') }}
UNION ALL
select '850-900', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 850 and ADD_QUANTITY_IN_LAST_WEEK < 900 then 1 else 0 end) 
from  {{ ref('dim_item') }}