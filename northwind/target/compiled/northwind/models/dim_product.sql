with stg_products as (
    select * from raw.northwind.Products
),
stg_categories as (
    select * from raw.northwind.Categories
)

select
    md5(cast(coalesce(cast(p.productid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as productkey,
    p.productid,
    p.productname,
    p.supplierid as supplierkey,
    c.categoryname,
    c.description as categorydescription
from stg_products p
left join stg_categories c
    on p.categoryid = c.categoryid