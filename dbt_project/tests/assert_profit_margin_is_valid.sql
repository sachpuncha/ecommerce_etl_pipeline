select *
from {{ ref('int_orders_cleaned') }}
where profit_margin_pct > 100
   or profit_margin_pct < -100