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
        cast(as_of_year as int64) as activity_year,

        -- Harmonize 2-digit FIPS state codes to 2-letter postal abbreviations
        case lpad(trim(safe_cast(state_code as string)), 2, '0')
            when '01' then 'AL'
            when '02' then 'AK'
            when '04' then 'AZ'
            when '05' then 'AR'
            when '06' then 'CA'
            when '08' then 'CO'
            when '09' then 'CT'
            when '10' then 'DE'
            when '11' then 'DC'
            when '12' then 'FL'
            when '13' then 'GA'
            when '15' then 'HI'
            when '16' then 'ID'
            when '17' then 'IL'
            when '18' then 'IN'
            when '19' then 'IA'
            when '20' then 'KS'
            when '21' then 'KY'
            when '22' then 'LA'
            when '23' then 'ME'
            when '24' then 'MD'
            when '25' then 'MA'
            when '26' then 'MI'
            when '27' then 'MN'
            when '28' then 'MS'
            when '29' then 'MO'
            when '30' then 'MT'
            when '31' then 'NE'
            when '32' then 'NV'
            when '33' then 'NH'
            when '34' then 'NJ'
            when '35' then 'NM'
            when '36' then 'NY'
            when '37' then 'NC'
            when '38' then 'ND'
            when '39' then 'OH'
            when '40' then 'OK'
            when '41' then 'OR'
            when '42' then 'PA'
            when '44' then 'RI'
            when '45' then 'SC'
            when '46' then 'SD'
            when '47' then 'TN'
            when '48' then 'TX'
            when '49' then 'UT'
            when '50' then 'VT'
            when '51' then 'VA'
            when '53' then 'WA'
            when '54' then 'WV'
            when '55' then 'WI'
            when '56' then 'WY'
            when '60' then 'AS'
            when '66' then 'GU'
            when '69' then 'MP'
            when '72' then 'PR'
            when '78' then 'VI'
            else null
        end as state_code,

        -- Standardized financial metrics (scaled from thousands to exact dollars)
        safe_cast(loan_amount_000s as float64) * 1000.0 as loan_amount,
        safe_cast(applicant_income_000s as float64) * 1000.0 as applicant_income,
        safe_cast(rate_spread as float64) as rate_spread,

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

        -- Applicant Demographics
        case safe_cast(applicant_race_1 as int64)
            when 1 then 'American Indian or Alaska Native'
            when 2 then 'Asian'
            when 3 then 'Black or African American'
            when 4 then 'Native Hawaiian or Other Pacific Islander'
            when 5 then 'White'
            when 6 then 'Information not provided'
            when 7 then 'Not applicable'
            else 'Unknown'
        end as applicant_race,

        case safe_cast(applicant_ethnicity as int64)
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
        case safe_cast(co_applicant_race_1 as int64)
            when 1 then 'American Indian or Alaska Native'
            when 2 then 'Asian'
            when 3 then 'Black or African American'
            when 4 then 'Native Hawaiian or Other Pacific Islander'
            when 5 then 'White'
            when 6 then 'Information not provided'
            when 7 then 'Not applicable'
            when 8 then 'No co-applicant'
            else 'Unknown'
        end as co_applicant_race,

        case safe_cast(co_applicant_ethnicity as int64)
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

    from {{ ref('stg_pre2018') }}

),

post2017 as (

    select
        cast(activity_year as int64) as activity_year,
        upper(trim(safe_cast(state_code as string))) as state_code,
        safe_cast(loan_amount as float64) as loan_amount,
        safe_cast(income as float64) * 1000.0 as applicant_income,
        safe_cast(rate_spread as float64) as rate_spread,

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
        case safe_cast(co_applicant_race_1 as int64)
            when 1 then 'American Indian or Alaska Native'
            when 2 then 'Asian'
            when 3 then 'Black or African American'
            when 4 then 'Native Hawaiian or Other Pacific Islander'
            when 5 then 'White'
            when 6 then 'Information not provided'
            when 7 then 'Not applicable'
            when 8 then 'No co-applicant'
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

    from {{ ref('stg_post2017') }}

),

unioned as (

    select * from pre2018
    union all
    select * from post2017

)

select * from unioned