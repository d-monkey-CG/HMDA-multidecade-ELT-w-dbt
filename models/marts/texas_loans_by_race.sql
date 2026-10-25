{{ config(materialized='table')}}

-- Layer 3: originated loans aggregated by race and year in Texas

select 
    activity_year,
    case 
        -- Combine Hispanic or Latino ethnicity and Race columns
        when applicant_ethnicity = 'Hispanic or Latino' 
            then 'Hispanic or Latino'

        -- Non-Hispanic racial groups
        when applicant_ethnicity = 'Not Hispanic or Latino' and applicant_race = 'White' 
            then 'Non-Hispanic White'
        when applicant_ethnicity = 'Not Hispanic or Latino' and applicant_race = 'Black or African American' 
            then 'Non-Hispanic Black'
        when applicant_ethnicity = 'Not Hispanic or Latino' and applicant_race = 'Asian' 
            then 'Asian'
        when applicant_ethnicity = 'Not Hispanic or Latino' and applicant_race = 'American Indian or Alaska Native' 
            then 'American Indian or Alaska Native'
        when applicant_ethnicity = 'Not Hispanic or Latino' and applicant_race = 'Native Hawaiian or Other Pacific Islander' 
            then 'Native Hawaiian or Other Pacific Islander'

        -- Fallbacks when ethnicity is missing/unknown but race is provided
        when applicant_race = 'White' 
            then 'Non-Hispanic White'
        when applicant_race = 'Black or African American' 
            then 'Non-Hispanic Black'
        when applicant_race = 'Asian' 
            then 'Asian'
        when applicant_race = 'American Indian or Alaska Native' 
            then 'American Indian or Alaska Native'
        when applicant_race = 'Native Hawaiian or Other Pacific Islander' 
            then 'Native Hawaiian or Other Pacific Islander'

        -- Unreported / Entity / Exempt
        when applicant_ethnicity in ('Information not provided', 'Not applicable')
            or applicant_race in ('Information not provided', 'Not applicable')
            then 'Jointly Unreported / NA'

        else 'Unknown'
    end as race_ethnicity_combined,
    count(*) as total_originated_loans

from {{ ref("int_hmda_harmonized") }}

    where action_taken = 'Loan originated' 
        and loan_purpose = 'Home purchase'
        and state_code = 'TX'
    group by 
        activity_year, 
        race_ethnicity_combined