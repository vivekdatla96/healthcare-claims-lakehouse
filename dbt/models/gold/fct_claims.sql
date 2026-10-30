-- One row per claim. Member attributes are taken as of the service date
-- (point-in-time join on the SCD2 dimension), not as of today.
with claims as (
    select * from {{ source('silver', 'claims') }}
)

select
    c.claim_id,
    c.member_token,
    c.provider_npi,
    c.service_date,
    date_trunc('month', c.service_date)::date              as service_month,
    c.icd10_code,
    c.cpt_code,
    c.claim_status,
    c.claim_status in ('PAID', 'DENIED')                   as is_adjudicated,
    c.billed_amount,
    c.allowed_amount,
    c.paid_amount,
    coalesce(c.allowed_amount - c.paid_amount, 0)          as member_responsibility,
    m.plan_code                                            as plan_code_at_service,
    m.member_state                                         as member_state_at_service,
    p.specialty,
    p.in_network,
    c._updated_at                                          as source_updated_at
from claims c
left join {{ ref('dim_member_scd') }} m
    on  m.member_token = c.member_token
    and c.service_date >= m.effective_from
    and c.service_date <  m.effective_to
left join {{ ref('dim_provider') }} p
    on p.provider_npi = c.provider_npi
