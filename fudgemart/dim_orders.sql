with stg_fm_orders as (
    select
        order_id,
        customer_id,
        order_date,
        shipped_date,
        ship_via,
        creditcard_id
    from {{ source('fudgemart_v3','fm_orders') }}
),

stg_ff_account_titles as (
    select
        at_id as order_id,
        at_account_id as customer_id,
        at_queue_date as order_date,
        at_shipped_date as shipped_date,
        at_returned_date as returned_date
    from {{ source('fudgeflix_v3','ff_account_titles') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'customer_id', 'order_date']) }} as orderkey,
    coalesce(fm.order_id, ff.order_id) as orderid,
    case 
        when fm.order_id is not null then 'Fudgemart'
        else 'Fudgeflix'
    end as division,
    coalesce(fm.order_date, ff.order_date) as order_date,
    coalesce(fm.shipped_date, ff.shipped_date) as shipped_date,
    ff.returned_date as returned_date,
    fm.ship_via as ship_via,
    coalesce(fm.customer_id, ff.customer_id) as customer_id
from stg_fm_orders fm
full outer join stg_ff_account_titles ff
    on fm.order_id = ff.order_id