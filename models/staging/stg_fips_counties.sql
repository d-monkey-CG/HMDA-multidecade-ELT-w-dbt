-- Ingest raw places seed
WITH source AS (
    SELECT * 
    FROM {{ ref('fips_reference_table') }}
),

-- Get distinct city-county pairs
distinct_cities AS (
    SELECT DISTINCT
        `StCnty FIPS Code`     AS county_fips,
        `State FIPS Code`      AS state_fips,
        `State Code`           AS state_abbr,
        INITCAP(`County Name`) AS county_name,
        INITCAP(`State Name`)  AS state_name,
        INITCAP(`City Name`)   AS city_name
    FROM source
    WHERE `StCnty FIPS Code` IS NOT NULL
      AND `City Name` IS NOT NULL
)

-- Group to 1 row per county, collecting all unique cities into an array
SELECT
    county_fips,
    state_fips,
    state_abbr,
    county_name,
    state_name,
    -- Creates a BigQuery ARRAY of all unique city names in this county
    ARRAY_AGG(city_name ORDER BY city_name) AS city_names
FROM distinct_cities
GROUP BY 
    county_fips, 
    state_fips, 
    state_abbr, 
    county_name, 
    state_name