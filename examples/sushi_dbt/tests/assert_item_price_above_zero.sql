select *
from {{ ref('items') }}
where price <= 0
