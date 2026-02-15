select
    anno,
    regione,
    pollutant,
    count(*) as n_stations,
    round(avg(annual_mean), 1) as avg_mean,
    round(max(annual_mean), 1) as worst_station_mean,
    round(avg(annual_max), 0) as avg_peak

from {{ ref('stg_air_quality') }}
where data_coverage >= 0.75  -- only stations with decent coverage
group by anno, regione, pollutant
order by anno, regione, pollutant
