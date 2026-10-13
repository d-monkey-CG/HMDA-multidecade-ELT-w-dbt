with source_data as (

    select * 
    from {{ source('raw_hmda', 'hmda_pre2018') }}

),

renamed as (

    select
        safe_cast(as_of_year as string) as as_of_year,
        safe_cast(state_code as string) as state_code,
        safe_cast(loan_type as string) as loan_type,
        safe_cast(property_type_name as string) as property_type_name,
        safe_cast(loan_amount_000s as string) as loan_amount_000s,
        safe_cast(action_taken as string) as action_taken,
        safe_cast(applicant_race_1 as string) as applicant_race_1,
        safe_cast(applicant_sex as string) as applicant_sex,
        safe_cast(applicant_ethnicity as string) as applicant_ethnicity,
        safe_cast(applicant_income_000s as string) as applicant_income_000s,
        safe_cast(rate_spread as string) as rate_spread,
        safe_cast(co_applicant_ethnicity as string) as co_applicant_ethnicity,
        safe_cast(co_applicant_race_1 as string) as co_applicant_race_1,
        safe_cast(co_applicant_sex as string) as co_applicant_sex,
        safe_cast(loan_purpose as string) as loan_purpose,
        safe_cast(denial_reason_1 as string) as denial_reason_1,
        safe_cast(denial_reason_2 as string) as denial_reason_2

    from source_data

)

select * from renamed
