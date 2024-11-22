
        
       insert into   dwh_sbx.psa_order_rk 
            select 
                src_system_cd,
    order_rk,
    client_rk,
    order_num
     
            from (select 
*
from "devdb"."dwh_sbx"."psa_order_bk" bk )bk
            inner  join dwh_sbx.psa_order_bk_rk bkrk on 
            bk.order_bk = bkrk.order_bk 
            
                
                    left join dwh_sbx.psa_client_bk_rk on 
                        bk.client_bk =  dwh_sbx.psa_client_bk_rk.client_bk 
                
             
            

    