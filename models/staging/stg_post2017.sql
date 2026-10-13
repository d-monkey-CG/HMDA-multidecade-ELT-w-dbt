with source_data as (

    select * 
    from `hmda-data-project.HMDA_ALL.hmda_post2017`

),

renamed as (

    select
        safe_cast(activity_year as string) as activity_year,
        safe_cast(state_code as string) as state_code,
        safe_cast(loan_type as string) as loan_type,
        safe_cast(derived_dwelling_category as string) as derived_dwelling_category,
        safe_cast(loan_amount as string) as loan_amount,
        safe_cast(action_taken as string) as action_taken,
        safe_cast(applicant_race_1 as string) as applicant_race_1,
        safe_cast(applicant_sex as string) as applicant_sex,
        safe_cast(applicant_ethnicity_1 as string) as applicant_ethnicity_1,
        safe_cast(income as string) as income,
        safe_cast(rate_spread as string) as rate_spread,
        safe_cast(co_applicant_ethnicity_1 as string) as co_applicant_ethnicity_1,
        safe_cast(co_applicant_race_1 as string) as co_applicant_race_1,
        safe_cast(co_applicant_sex as string) as co_applicant_sex,
        safe_cast(loan_purpose as string) as loan_purpose,
        safe_cast(denial_reason_1 as string) as denial_reason_1,
        safe_cast(denial_reason_2 as string) as denial_reason_2

    from source_data

)

select * from renamed
