with source as (
    select * from {{ source('raw', 'waste') }}
),

parsed as (
    select
        anno,
        IstatComune as istat_code,
        Regione as regione,
        Provincia as provincia,
        Comune as comune,
        try_cast(replace(Popolazione, '.', '') as int) as popolazione,

        -- Separate collection by material (tonnes)
        {{ parse_italian_number('"Frazione umida(1) (t)"') }} as organic_t,
        {{ parse_italian_number('"Verde (t)"') }} as green_waste_t,
        {{ parse_italian_number('"Carta e cartone (t)"') }} as paper_t,
        {{ parse_italian_number('"Vetro (t)"') }} as glass_t,
        {{ parse_italian_number('"Legno (t)"') }} as wood_t,
        {{ parse_italian_number('"Metallo (t)"') }} as metal_t,
        {{ parse_italian_number('"Plastica (t)"') }} as plastic_t,
        {{ parse_italian_number('"RAEE (t)"') }} as weee_t,
        {{ parse_italian_number('"Tessili (t)"') }} as textiles_t,

        -- Totals
        {{ parse_italian_number('"Totale RD (t)"') }} as total_separate_collection_t,
        {{ parse_italian_number('"Indifferenziato (t)"') }} as unsorted_waste_t,
        {{ parse_italian_number('"Totale RU (t)"') }} as total_waste_t,
        {{ parse_italian_pct('"Percentuale RD (%)"') }} as recycling_rate_pct

    from source
    where Comune is not null
)

select
    *,
    case when popolazione > 0
        then round(total_waste_t * 1000 / popolazione, 1)
    end as waste_per_capita_kg
from parsed
