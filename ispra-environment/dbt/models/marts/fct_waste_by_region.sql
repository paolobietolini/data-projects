select
    anno,
    regione,
    count(*) as n_comuni,
    sum(popolazione) as popolazione,
    round(sum(total_waste_t), 0) as total_waste_t,
    round(sum(total_separate_collection_t), 0) as total_separate_collection_t,
    round(sum(unsorted_waste_t), 0) as unsorted_waste_t,
    round(sum(total_separate_collection_t) / nullif(sum(total_waste_t), 0) * 100, 1) as recycling_rate_pct,
    round(sum(total_waste_t) * 1000 / nullif(sum(popolazione), 0), 1) as waste_per_capita_kg

from {{ ref('stg_waste') }}
group by anno, regione
order by anno, regione
