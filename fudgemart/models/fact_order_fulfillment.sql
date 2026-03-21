with f as (
    select
        source_order_id as fulfillment_id,
        source_customer_id as customer_key,
        source_item_id as item_key,
        source_order_id as order_key,

        -- just pass through the raw dates for now
        order_date,
        shipped_date,
        returned_date,

        quantity,
        unit_price,
        quantity * unit_price as extended_price,
        fulfillment_channel,
        source_system
    from {{ ref('stg_fulfillment_events') }}
    qualify row_number() over (partition by source_system order by order_date) <= 5
)


select * from f
order by source_system, fulfillment_id