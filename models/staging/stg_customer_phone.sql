select
    customer_id,
    (p->>'id')::uuid  as phone_id,
    p->>'phone'       as phone
from src
cross join lateral jsonb_array_elements(c->'phones') p
