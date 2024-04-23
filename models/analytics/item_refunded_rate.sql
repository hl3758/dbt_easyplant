select '0.2-0.3',
sum(case when REFUNDED_RETURNED_RATE_LAST_WEEK < 0.3 then 1 else 0 end) as amount
from  {{ ref('dim_item') }}
UNION ALL
select '0.3-0.4', 
sum(case when REFUNDED_RETURNED_RATE_LAST_WEEK > 0.3 and REFUNDED_RETURNED_RATE_LAST_WEEK < 0.4 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '0.4-0.5', 
sum(case when REFUNDED_RETURNED_RATE_LAST_WEEK > 0.4 and REFUNDED_RETURNED_RATE_LAST_WEEK < 0.5 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '0.5-0.6', 
sum(case when REFUNDED_RETURNED_RATE_LAST_WEEK > 0.5 and REFUNDED_RETURNED_RATE_LAST_WEEK < 0.6 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select '0.6-0.7', 
sum(case when REFUNDED_RETURNED_RATE_LAST_WEEK > 0.6 then 1 else 0 end)
from 
{{ ref('dim_item') }}