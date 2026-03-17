
    
    

select
    productid as unique_field,
    count(*) as n_records

from analytics.dbt_jmlikota_northwind.dim_product
where productid is not null
group by productid
having count(*) > 1


