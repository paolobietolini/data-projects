with no2 as (
    select
        station_eu_code, nome_stazione, Regione, Provincia, Comune,
        tipo_zona, tipo_stazione, Lon, Lat, yy,
        cast(n as int) as n_measurements,
        media_yy, massimo, perc50, copertura,
        'NO2' as pollutant
    from {{ source('raw', 'air_no2') }}
),

pm10 as (
    select
        station_eu_code, nome_stazione, Regione, Provincia, Comune,
        tipo_zona, tipo_stazione, Lon, Lat, yy,
        cast(n as int) as n_measurements,
        media_yy, massimo, perc50, copertura,
        'PM10' as pollutant
    from {{ source('raw', 'air_pm10') }}
),

pm25 as (
    select
        station_eu_code, nome_stazione, Regione, Provincia, Comune,
        tipo_zona, tipo_stazione, Lon, Lat, yy,
        cast(n as int) as n_measurements,
        media_yy, massimo, perc50, copertura,
        'PM2.5' as pollutant
    from {{ source('raw', 'air_pm25') }}
),

o3 as (
    select
        station_eu_code, nome_stazione, Regione, Provincia, Comune,
        tipo_zona, tipo_stazione, Lon, Lat, yy,
        cast(n as int) as n_measurements,
        media_yy, massimo, perc50, copertura,
        'O3' as pollutant
    from {{ source('raw', 'air_o3') }}
),

unioned as (
    select * from no2
    union all
    select * from pm10
    union all
    select * from pm25
    union all
    select * from o3
),

renamed as (
    select
        station_eu_code,
        nome_stazione as station_name,
        pollutant,
        Regione as regione,
        Provincia as provincia,
        Comune as comune,
        tipo_zona as zone_type,
        tipo_stazione as station_type,
        cast(Lon as double) as longitude,
        cast(Lat as double) as latitude,
        cast(yy as int) as anno,
        n_measurements,
        cast(media_yy as double) as annual_mean,
        cast(massimo as double) as annual_max,
        cast(perc50 as double) as median,
        cast(copertura as double) as data_coverage

    from unioned
    where yy is not null
)

select * from renamed
