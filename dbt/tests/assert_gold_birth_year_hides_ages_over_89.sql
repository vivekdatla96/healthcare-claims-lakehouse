-- Fails if gold exposes a birth year that implies an age of 90 or over
-- (HIPAA Safe Harbor requires those to be aggregated).
select 'dim_member_scd' as model, member_token, birth_year
from {{ ref('dim_member_scd') }}
where {{ age_from_birth_year('birth_year') }} >= 90
union all
select 'dim_member' as model, member_token, birth_year
from {{ ref('dim_member') }}
where {{ age_from_birth_year('birth_year') }} >= 90
