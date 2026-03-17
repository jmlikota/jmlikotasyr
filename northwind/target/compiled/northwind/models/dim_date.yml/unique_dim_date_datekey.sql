
    
    

select
    datekey as unique_field,
    count(*) as n_records

from analytics.dbt_jmlikota_northwind.dim_date
where datekey is not null
group by datekey
having count(*) > 1


