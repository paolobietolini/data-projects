select
    istat_code,
    comune,
    provincia,
    regione,
    consumed_soil_2024_ha,
    consumed_soil_2024_pct,
    total_net_increment_2006_2024_ha,
    -- Recent trend (last 3 years)
    (net_increment_2021_2022_ha + net_increment_2022_2023_ha + net_increment_2023_2024_ha) as net_increment_last_3y_ha,
    row_number() over (order by consumed_soil_2024_ha desc) as rank_by_total_ha,
    row_number() over (order by consumed_soil_2024_pct desc) as rank_by_pct,
    row_number() over (order by total_net_increment_2006_2024_ha desc) as rank_by_growth

from {{ ref('stg_land_consumption') }}
where consumed_soil_2024_ha is not null
