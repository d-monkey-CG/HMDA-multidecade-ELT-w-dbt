-- models/staging/stg_fips_counties.sql

WITH source AS (
    SELECT * FROM {{ ref('raw_fips_places') }}
),

deduped AS (
    SELECT
        `StCnty FIPS Code`                                        AS county_code,
        `State FIPS Code`                                         AS state_fips,
        `State Code`                                              AS state_abbr,
        INITCAP(`County Name`)                                    AS county_name,
        INITCAP(`State Name`)                                     AS state_name,
        ROW_NUMBER() OVER (
            PARTITION BY `StCnty FIPS Code` 
            ORDER BY `City Name`
        ) AS row_num
    FROM source
    WHERE `StCnty FIPS Code` IS NOT NULL
)

SELECT
    county_code,
    state_fips,
    state_abbr,
    county_name,
    state_name
FROM deduped
WHERE row_num = 1