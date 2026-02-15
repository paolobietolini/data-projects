with source as (
    select * from {{ source('raw', 'land_consumption') }}
),

renamed as (
    select
        PRO_COM as istat_code,
        Nome_Comune as comune,
        Nome_Regione as regione,
        Nome_Provincia as provincia,

        -- Net increments per period (hectares)
        "Incremento netto 2006-2012 [ettari]" as net_increment_2006_2012_ha,
        "Incremento netto 2012-2015 [ettari]" as net_increment_2012_2015_ha,
        "Incremento netto 2015-2016 [ettari]" as net_increment_2015_2016_ha,
        "Incremento netto 2016-2017 [ettari]" as net_increment_2016_2017_ha,
        "Incremento netto 2017-2018 [ettari]" as net_increment_2017_2018_ha,
        "Incremento netto 2018-2019 [ettari]" as net_increment_2018_2019_ha,
        "Incremento netto 2019-2020 [ettari]" as net_increment_2019_2020_ha,
        "Incremento netto 2020-2021 [ettari]" as net_increment_2020_2021_ha,
        "Incremento netto 2021-2022 [ettari]" as net_increment_2021_2022_ha,
        "Incremento netto 2022-2023 [ettari]" as net_increment_2022_2023_ha,
        "Incremento netto 2023-2024 [ettari]" as net_increment_2023_2024_ha,

        -- Total consumed soil 2024
        "Suolo consumato 2024 [ettari]" as consumed_soil_2024_ha,
        "Suolo consumato 2024 [%]" as consumed_soil_2024_pct,

        -- Derived: total net increment 2006-2024
        (
            "Incremento netto 2006-2012 [ettari]"
            + "Incremento netto 2012-2015 [ettari]"
            + "Incremento netto 2015-2016 [ettari]"
            + "Incremento netto 2016-2017 [ettari]"
            + "Incremento netto 2017-2018 [ettari]"
            + "Incremento netto 2018-2019 [ettari]"
            + "Incremento netto 2019-2020 [ettari]"
            + "Incremento netto 2020-2021 [ettari]"
            + "Incremento netto 2021-2022 [ettari]"
            + "Incremento netto 2022-2023 [ettari]"
            + "Incremento netto 2023-2024 [ettari]"
        ) as total_net_increment_2006_2024_ha

    from source
)

select * from renamed
