{{ config(materialized='table') }}

-- Layer 3: originated loans aggregated by applicants with a co-applicant, 
-- single men, and single women

select 
    activity_year, 
    applicant_sex, 
    case co_applicant_sex
        when 'No co-applicant' then 'single'
        when 'Not applicable' then 'single'
        else 'with co-borrower'
    end as borrower_status,
    count(*) as total_originated_loans,

from {{ ref("int_hmda_harmonized") }}

    where action_taken = 'Loan originated' AND loan_purpose = 'Home purchase'
    group by 
        activity_year, 
        applicant_sex, 
        borrower_status


