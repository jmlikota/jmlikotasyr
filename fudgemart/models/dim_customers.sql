/* STAGING VIEW/TABLE: stg_customers
   Purpose: Unify FudgeFlix and FudgeMart customer data.
*/
with 
stg_customers as (
--Stage FudgeFlix Accounts with ZIP lookup
    select
        a.account_id as source_customer_id,
        'FudgeFlix' as source_system,
        a.account_firstname as contact_first_name,
        a.account_lastname as contact_last_name,
        a.account_address as customer_address,
        z.zip_city as customer_city,       -- from ff_zipcodes
        z.zip_state as customer_state,     -- from ff_zipcodes
        a.account_zipcode as customer_postal_code,
        NULL as customer_phone,            -- missing in FudgeFlix
        a.account_email as customer_email
    from {{ source('fudgeflix_v3','ff_accounts') }} a
    left join {{ source('fudgeflix_v3','ff_zipcodes') }} z
        on a.account_zipcode = z.zip_code

    union all

    --Stage FudgeMart Customers
    select
        c.customer_id as source_customer_id,
        'FudgeMart' as source_system,
        c.customer_firstname as contact_first_name,
        c.customer_lastname as contact_last_name,
        c.customer_address as customer_address,
        c.customer_city as customer_city,
        c.customer_state as customer_state,   -- standardize state column
        c.customer_zip as customer_postal_code,
        c.customer_phone as customer_phone,
        c.customer_email as customer_email
    from {{ source('fudgemart_v3','fm_customers') }} c
)

select
    {{ dbt_utils.generate_surrogate_key(['source_customer_id','source_system']) }} as customerkey,
    *
from stg_customers