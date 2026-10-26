{{ config(severity='warn') }}

-- The zip code must belong to the range of the state: the test fails if this query returns rows
select
    u.user_id,
    u.state,
    u.zip_code,
    s.zip_min,
    s.zip_max
from {{ ref('stg_users') }} u
join {{ ref('states') }} s on u.state = s.code
where cast(u.zip_code as integer) not between s.zip_min and s.zip_max
