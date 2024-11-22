
    
    

select
    id as unique_field,
    count(*) as n_records

from "devdb"."dwh_sbx"."psa2_dbt_model"
where id is not null
group by id
having count(*) > 1


