select
    npi                 as provider_npi,
    provider_name,
    specialty,
    state               as provider_state,
    in_network,
    _updated_at         as source_updated_at
from {{ source('silver', 'providers') }}
