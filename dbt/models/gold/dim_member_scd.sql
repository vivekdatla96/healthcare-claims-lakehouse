-- Member versions with closed-open effective windows.
-- The first version is back-dated so claims with service dates before the
-- member's first load still resolve to a version.
select
    member_token,
    birth_year,
    gender,
    zip3,
    state                as member_state,
    plan_code,
    case
        when row_number() over (partition by member_token order by valid_from) = 1
            then timestamp '1900-01-01'
        else valid_from
    end                  as effective_from,
    coalesce(valid_to, timestamp '9999-12-31') as effective_to,
    is_current
from {{ source('silver', 'member_history') }}
