select 'a_first_range',
sum(case when ADD_QUANTITY_IN_LAST_WEEK < 650 then 1 else 0 end) as amount
from  {{ ref('dim_item') }}
UNION ALL
select 'b_second_range', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 650 and ADD_QUANTITY_IN_LAST_WEEK < 700 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select 'c_third_range', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 700 and ADD_QUANTITY_IN_LAST_WEEK < 750 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select 'd_forth_range', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 750 and ADD_QUANTITY_IN_LAST_WEEK < 800 then 1 else 0 end) 
from 
{{ ref('dim_item') }}
UNION ALL
select 'e_fifth_range', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 800 and ADD_QUANTITY_IN_LAST_WEEK < 850 then 1 else 0 end)
from 
{{ ref('dim_item') }}
UNION ALL
select 'f_sixth_range', 
sum(case when ADD_QUANTITY_IN_LAST_WEEK > 850 and ADD_QUANTITY_IN_LAST_WEEK < 900 then 1 else 0 end) 
from  {{ ref('dim_item') }}