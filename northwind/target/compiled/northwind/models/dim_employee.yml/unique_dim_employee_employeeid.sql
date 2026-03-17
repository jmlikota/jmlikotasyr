
    
    

select
    employeeid as unique_field,
    count(*) as n_records

from analytics.dbt_jmlikota_northwind.dim_employee
where employeeid is not null
group by employeeid
having count(*) > 1


