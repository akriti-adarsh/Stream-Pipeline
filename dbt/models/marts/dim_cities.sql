-- City dimension from the seed; one row per simulated city.
select
    city_id,
    name as city_name,
    center_lat,
    center_lon,
    demand_weight
from {{ ref('cities') }}
