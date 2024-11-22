/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/




select 
NULL as  clinet_rk,
BK.clinet_bk
from "devdb"."dwh_sbx"."psa_clinet_bk" BK

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null