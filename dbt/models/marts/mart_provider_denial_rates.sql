select
    f.provider_npi,
    p.provider_name,
    p.specialty,
    p.in_network,
    count(*) filter (where f.is_adjudicated)                   as adjudicated_claims,
    count(*) filter (where f.claim_status = 'DENIED')          as denied_claims,
    round(
        count(*) filter (where f.claim_status = 'DENIED')
        / nullif(count(*) filter (where f.is_adjudicated), 0), 4
    )                                                          as denial_rate
from {{ ref('fct_claims') }} f
join {{ ref('dim_provider') }} p using (provider_npi)
group by 1, 2, 3, 4
