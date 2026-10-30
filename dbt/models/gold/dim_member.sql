select
    member_token,
    birth_year,
    gender,
    zip3,
    member_state,
    plan_code,
    case
        when {{ age_from_birth_year('birth_year') }} < 18 then '0-17'
        when {{ age_from_birth_year('birth_year') }} < 35 then '18-34'
        when {{ age_from_birth_year('birth_year') }} < 50 then '35-49'
        when {{ age_from_birth_year('birth_year') }} < 65 then '50-64'
        else '65+'
    end as age_band
from {{ ref('dim_member_scd') }}
where is_current
