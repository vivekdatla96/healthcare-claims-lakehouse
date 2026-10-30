select
    service_month,
    specialty,
    count(*)                                                   as claim_count,
    count(*) filter (where claim_status = 'PAID')              as paid_claims,
    sum(billed_amount)                                         as total_billed,
    sum(paid_amount)                                           as total_paid,
    round(sum(paid_amount) / nullif(sum(billed_amount), 0), 4) as paid_to_billed_ratio
from {{ ref('fct_claims') }}
group by 1, 2
