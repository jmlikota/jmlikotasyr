
    
    

select
    date as unique_field,
    count(*) as n_records

from analytics.dbt_jmlikota_northwind.dim_date
where date is not null
group by date
having count(*) > 1


