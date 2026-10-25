with source_data as (

    select * 
    from `hmda-data-project.HMDA_ALL.hmda_post2017`

),

renamed as (

    select
        cast(activity_year as int64) as activity_year,
        upper(trim(safe_cast(state_code as string))) as state_code,
        safe_cast(loan_amount as float64) as loan_amount,
        safe_cast(income as float64) * 1000.0 as applicant_income,
        safe_cast(rate_spread as float64) as rate_spread,
        safe_cast(county_code as string) as county_code,
        safe_cast(census_tract as string) as census_tract,
        safe_cast(business_or_commercial_purpose as int64) as business_or_commercial_purpose,
        safe_cast(interest_rate as float64) as interest_rate,
        -- Handles potential decimals like "250000.00" before converting to integer
        cast(safe_cast(property_value as numeric) as int64) as property_value,
        safe_cast(debt_to_income_ratio as string) as debt_to_income_ratio,
        safe_cast(applicant_age as string) as applicant_age,
        safe_cast(co_applicant_age as string) as co_applicant_age,
        safe_cast(tract_median_age_of_housing_units as int64) as tract_median_age_of_housing_units,
        
        -- Loan Characteristics
        case safe_cast(loan_type as int64)
            when 1 then 'Conventional'
            when 2 then 'FHA-insured'
            when 3 then 'VA-guaranteed'
            when 4 then 'FSA/RHS'
            else 'Unknown'
        end as loan_type,

        case safe_cast(loan_purpose as int64)
            when 1 then 'Home purchase'
            when 2 then 'Home improvement'
            when 31 then 'Refinancing'
            when 32 then 'Refinancing'
            when 3 then 'Refinancing'
            else 'Other / Not applicable'
        end as loan_purpose,

        -- Action Taken
        case safe_cast(action_taken as int64)
            when 1 then 'Loan originated'
            when 2 then 'Application approved but not accepted'
            when 3 then 'Application denied'
            when 4 then 'Application withdrawn'
            when 5 then 'File closed for incompleteness'
            when 6 then 'Loan purchased by institution'
            when 7 then 'Preapproval denied'
            when 8 then 'Preapproval approved not accepted'
            else 'Unknown'
        end as action_taken,

        -- Applicant Demographics (Harmonized Race Classification)
        case 
            when safe_cast(applicant_race_1 as int64) = 1 then 'American Indian or Alaska Native'
            when safe_cast(applicant_race_1 as int64) in (2, 21, 22, 23, 24, 25, 26, 27) then 'Asian'
            when safe_cast(applicant_race_1 as int64) = 3 then 'Black or African American'
            when safe_cast(applicant_race_1 as int64) in (4, 41, 42, 43, 44) then 'Native Hawaiian or Other Pacific Islander'
            when safe_cast(applicant_race_1 as int64) = 5 then 'White'
            when safe_cast(applicant_race_1 as int64) = 6 then 'Information not provided'
            when safe_cast(applicant_race_1 as int64) = 7 then 'Not applicable'
            else 'Unknown'
        end as applicant_race,

        case safe_cast(applicant_ethnicity_1 as int64)
            when 1 then 'Hispanic or Latino'
            when 2 then 'Not Hispanic or Latino'
            when 3 then 'Information not provided'
            when 4 then 'Not applicable'
            else 'Unknown'
        end as applicant_ethnicity,

        case safe_cast(applicant_sex as int64)
            when 1 then 'Male'
            when 2 then 'Female'
            when 3 then 'Information not provided'
            when 4 then 'Not applicable'
            else 'Unknown'
        end as applicant_sex,

        -- Co-Applicant Demographics
        case 
            when safe_cast(co_applicant_race_1 as int64) = 1 then 'American Indian or Alaska Native'
            when safe_cast(co_applicant_race_1 as int64) in (2, 21, 22, 23, 24, 25, 26, 27) then 'Asian'
            when safe_cast(co_applicant_race_1 as int64) = 3 then 'Black or African American'
            when safe_cast(co_applicant_race_1 as int64) in (4, 41, 42, 43, 44) then 'Native Hawaiian or Other Pacific Islander'
            when safe_cast(co_applicant_race_1 as int64) = 5 then 'White'
            when safe_cast(co_applicant_race_1 as int64) = 6 then 'Information not provided'
            when safe_cast(co_applicant_race_1 as int64) = 7 then 'Not applicable'
            when safe_cast(co_applicant_race_1 as int64) = 8 then 'No co-applicant'
            else 'Unknown'
        end as co_applicant_race,

        case safe_cast(co_applicant_ethnicity_1 as int64)
            when 1 then 'Hispanic or Latino'
            when 2 then 'Not Hispanic or Latino'
            when 3 then 'Information not provided'
            when 4 then 'Not applicable'
            when 5 then 'No co-applicant'
            else 'Unknown'
        end as co_applicant_ethnicity,

        case safe_cast(co_applicant_sex as int64)
            when 1 then 'Male'
            when 2 then 'Female'
            when 3 then 'Information not provided'
            when 4 then 'Not applicable'
            when 5 then 'No co-applicant'
            else 'Unknown'
        end as co_applicant_sex,

        -- Primary Denial Reasons
        case safe_cast(denial_reason_1 as int64)
            when 1 then 'Debt-to-income ratio'
            when 2 then 'Employment history'
            when 3 then 'Credit history'
            when 4 then 'Collateral'
            when 5 then 'Insufficient cash'
            when 6 then 'Unverifiable information'
            when 7 then 'Credit application incomplete'
            when 8 then 'Mortgage insurance denied'
            when 9 then 'Other'
            else null
        end as primary_denial_reason

    from source_data

)

select * from renamed
