with source_data as (
    select * 
    from {{ source('raw_hmda', 'hud_crosswalk') }}
),

renamed as (
    select 
        zip as zipcode,
        -- select only the last six digits of each geoid
        RIGHT(geoid, 6) as census_tract,
        -- select the first 5 digits of each geoid
        LEFT(geoid, 5) as fips_county_code,
        res_ratio,
        tot_ratio,
        cast(hmda_year as int64) as year 

    from source_data
)

select 
    zipcode,
    census_tract,
    fips_county_code,
    res_ratio,
    tot_ratio,
    year
from renamed