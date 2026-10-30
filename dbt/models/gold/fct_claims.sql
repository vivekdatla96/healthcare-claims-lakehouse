-- One row per claim. Member attributes are taken as of the service date
-- (point-in-time join on the SCD2 dimension), not as of today.
--
-- Incremental: a run rebuilds only claims whose own row, member history or
-- provider changed at or after the latest change already reflected in this
-- table (>= so equal source timestamps are never skipped; reprocessing is idempotent).
-- Claims deleted in silver are removed by the post-hook.
{{
    config(
        materialized='incremental',
        unique_key='claim_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        post_hook=[
            "{% if is_incremental() %}delete from {{ this }} f where not exists (select 1 from {{ source('silver', 'claims') }} s where s.claim_id = f.claim_id){% endif %}"
        ]
    )
}}

with claims as (
    select * from {{ source('silver', 'claims') }}
),

member_changes as (
    select
        member_token,
        max(greatest(valid_from, coalesce(valid_to, valid_from))) as member_updated_at
    from {{ source('silver', 'member_history') }}
    group by member_token
),

joined as (
    select
        c.*,
        mc.member_updated_at,
        p.source_updated_at as provider_updated_at,
        p.provider_npi is null as provider_missing
    from claims c
    left join member_changes mc
        on mc.member_token = c.member_token
    left join {{ ref('dim_provider') }} p
        on p.provider_npi = c.provider_npi
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
    c._updated_at                                          as source_updated_at,
    greatest(
        c._updated_at,
        coalesce(c.member_updated_at, c._updated_at),
        coalesce(c.provider_updated_at, c._updated_at)
    )                                                      as lineage_updated_at
from joined c
left join {{ ref('dim_member_scd') }} m
    on  m.member_token = c.member_token
    and c.service_date >= m.effective_from
    and c.service_date <  m.effective_to
left join {{ ref('dim_provider') }} p
    on p.provider_npi = c.provider_npi
{% if is_incremental() %}
where greatest(
        c._updated_at,
        coalesce(c.member_updated_at, c._updated_at),
        coalesce(c.provider_updated_at, c._updated_at)
      ) >= (select coalesce(max(lineage_updated_at), timestamp '1900-01-01') from {{ this }})
   or c.provider_missing
{% endif %}
