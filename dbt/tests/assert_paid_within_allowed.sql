-- Fails if any claim pays more than it allows.
select claim_id, allowed_amount, paid_amount
from {{ ref('fct_claims') }}
where paid_amount > allowed_amount
