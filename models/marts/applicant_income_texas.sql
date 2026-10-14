{{ config(materialized='table') }}

-- Layer 3: Originated Loans Applicant Income Distribution In Texas

select  
    activity_year,
    state_code, 
    applicant_income

from {{ ref("int_hmda_harmonized")}}

where action_taken = 'Loan originated' 
    and loan_purpose = 'Home purchase' 
    and state_code = 'TX' 
    and activity_year IN (2021, 2025)
    and applicant_income is not null
    and applicant_income > 0