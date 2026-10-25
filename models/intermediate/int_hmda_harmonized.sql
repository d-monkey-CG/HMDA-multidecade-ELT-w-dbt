{{ config(
    materialized='table',
    partition_by={
      "field": "activity_year",
      "data_type": "int64",
      "range": {
        "start": 2004,
        "end": 2026,
        "interval": 1
      }
    },
    cluster_by=["state_code", "action_taken", "loan_type"]
) }}

with pre2018 as (

    select
        activity_year,
        state_code,
        -- Standardized financial metrics (scaled from thousands to exact dollars)
        loan_amount,
        applicant_income,
        rate_spread,
        -- Loan Characteristics
        loan_type,
        loan_purpose,
        -- Action Taken
        action_taken,
        applicant_race,
        applicant_ethnicity,
        applicant_sex,
        -- Co-Applicant Demographics
        co_applicant_race,
        co_applicant_ethnicity,
        co_applicant_sex,
        -- Primary Denial Reasons
        primary_denial_reason

    from {{ ref('stg_pre2018') }}

),

post2017 as (

    select
        activity_year,
        state_code,
        loan_amount,
        applicant_income,
        rate_spread,
        -- Loan Characteristics
        loan_type,
        loan_purpose,
        -- Action Taken
        action_taken,
        -- Applicant Demographics (Harmonized Race Classification)
        applicant_race,
        applicant_ethnicity,
        applicant_sex,
        -- Co-Applicant Demographics
        co_applicant_race,
        co_applicant_ethnicity,
        co_applicant_sex,
        -- Primary Denial Reasons
        primary_denial_reason

    from {{ ref('stg_post2017') }}

),

unioned as (

    select * from pre2018
    union all
    select * from post2017

)

select * from unioned