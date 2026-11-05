{{ config(
    materialized='view'
) }}

with crosswalk_deduped as (
    -- Take the ZIP code with the highest residential ratio to prevent loan row duplication
    select
        census_tract,
        zipcode
    from (
        select
            census_tract,
            zipcode,
            row_number() over (
                partition by census_tract 
                order by res_ratio desc
            ) as rn
        from {{ ref('stg_dim_hud_crosswalk') }}
    )
    where rn = 1
)

select 
    sp.activity_year,
    sp.state_code,
    sp.county_code,
    sp.loan_amount,
    sp.applicant_income,
    sp.rate_spread,
    sp.interest_rate,
    sp.property_value,
    sp.applicant_age,
    sp.co_applicant_age,
    sp.tract_median_age_of_housing_units as area_median_age,
    sp.loan_type,
    sp.loan_purpose,
    sp.action_taken,
    sp.applicant_race,
    sp.applicant_ethnicity,
    sp.applicant_sex,
    sp.co_applicant_race,
    sp.co_applicant_ethnicity,
    sp.co_applicant_sex,
    sp.primary_denial_reason,
    sp.census_tract,
    sdhc.zipcode

from {{ ref('stg_post2017') }} as sp
left join crosswalk_deduped as sdhc
    on sp.census_tract = sdhc.census_tract