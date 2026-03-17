with
-- Stage Fudgemart orders
stg_fm_orders as (
    select
        order_id,
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as orderkey,
        customer_id as customerkey,
        'Fudgemart' as division,
        replace(to_date(order_date)::varchar, '-', '')::int as orderdatekey,
        replace(to_date(shipped_date)::varchar, '-', '')::int as shippeddatekey,
        null as returneddatekey,
        ship_via
    from {{ source('fudgemart_v3','fm_orders') }}
),

-- Stage Fudgeflix orders
stg_ff_orders as (
    select
        at_id as order_id,
        {{ dbt_utils.generate_surrogate_key(['at_id']) }} as orderkey,
        at_account_id as customerkey,
        'Fudgeflix' as division,
        replace(to_date(at_queue_date)::varchar, '-', '')::int as orderdatekey,
        replace(to_date(at_shipped_date)::varchar, '-', '')::int as shippeddatekey,
        replace(to_date(at_returned_date)::varchar, '-', '')::int as returneddatekey,
        null as ship_via
    from {{ source('fudgeflix_v3','ff_account_titles') }}
),

-- Combine both sources
all_orders as (
    select * from stg_fm_orders
    union all
    select * from stg_ff_orders
)

select *
from all_orders