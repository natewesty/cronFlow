select
  customer_id,
  (cl->>'clubMembershipId')::uuid as club_membership_id,
  (cl->>'clubId')::uuid           as club_id,
  cl->>'clubTitle'                as club_title,
  (cl->>'signupDate')::timestamptz as signup_at,
  (cl->>'cancelDate')::timestamptz as cancel_at
from base
cross join lateral jsonb_array_elements(c->'clubs') cl
