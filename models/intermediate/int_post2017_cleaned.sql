{{ config(
    materialized='view'
) }}

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
left join {{ ref('stg_dim_hud_crosswalk') }} as sdhc
    on sp.census_tract = sdhc.census_tract