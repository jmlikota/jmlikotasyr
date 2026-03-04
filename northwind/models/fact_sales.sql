with
stg_orders as (
    select
        OrderID,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey
    from {{ source('northwind', 'Orders') }}
),

stg_order_details as (
    select
        OrderID,
        productid,
        sum(quantity) as quantity,
        sum(quantity * unitprice) as extendedpriceamount,
        sum(quantity * unitprice * discount) as discountamount
    from {{ source('northwind', 'Order_Details') }}
    group by OrderID, productid
),

stg_products as (
    select
        productkey,
        productid
    from {{ ref('dim_products') }}
)

select
    o.orderid,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    p.productkey,
    od.quantity,
    od.extendedpriceamount,
    od.discountamount,
    od.extendedpriceamount - od.discountamount as soldamount
from stg_orders o
join stg_order_details od on o.OrderID = od.OrderID
join stg_products p on od.productid = p.productid