with
stg_orders as (
    select
        OrderID,
        md5(cast(coalesce(cast(employeeid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as employeekey,
        md5(cast(coalesce(cast(customerid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customerkey,
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey
    from raw.northwind.Orders
),

stg_order_details as (
    select
        OrderID,
        productid,
        sum(quantity) as quantity,
        sum(quantity * unitprice) as extendedpriceamount,
        sum(quantity * unitprice * discount) as discountamount
    from raw.northwind.Order_Details
    group by OrderID, productid
),

stg_products as (
    select
        productkey,
        productid
    from analytics.dbt_jmlikota_northwind.dim_product
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