{{ config(
    materialized='view',
    partition_by={
      "field": "activity_year",
      "data_type": "int64",
      "range": {
        "start": 2018,
        "end": 2026,
        "interval": 1
      }
    },
    cluster_by=["state_code", "county_code", "census_tract"]
) }}

select 
    activity_year,
    state_code,
    loan_amount,
    applicant_income,
    rate_spread,
    interest_rate,
    property_value,
    applicant_age,
    co_applicant_age,
    tract_median_age_of_housing_units as home_median_age,
    loan_type,
    loan_purpose,
    action_taken,
    applicant_race,
    applicant_ethnicity,
    applicant_sex,
    co_applicant_race,
    co_applicant_ethnicity,
    co_applicant_sex,
    primary_denial_reason,

    -- county_code to county name
    

from {{ ref('stg_post2017') }}