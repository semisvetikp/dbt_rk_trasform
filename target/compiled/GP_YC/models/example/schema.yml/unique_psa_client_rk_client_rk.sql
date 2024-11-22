
    
    

select
    client_rk as unique_field,
    count(*) as n_records

from "devdb"."dwh_sbx"."psa_client_rk"
where client_rk is not null
group by client_rk
having count(*) > 1


