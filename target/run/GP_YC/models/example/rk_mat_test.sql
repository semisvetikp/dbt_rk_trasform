
        
       insert into   dwh_sbx.rk_mat_test 
            select 
                src_system_cd,
    client_rk,
    client_fio
     
            from (select 
*
from "devdb"."dwh_sbx"."psa_client_bk" bk )bk
            inner  join dwh_sbx.psa_client_bk_rk bkrk on 
            bk.client_bk = bkrk.client_bk 
            

    