-- Use the `ref` function to select from other models
select 
src_system_cd,
client_rk,
client_fio
from "devdb"."dwh_sbx"."psa_client_bk" bk
inner  join "devdb"."dwh_sbx"."psa_client_bk_rk" bkrk on 
bk.client_bk = bkrk.client_bk