select 
md5(BK.client_bk) as client_rk,
BK.client_bk
from  "devdb"."dwh_sbx"."psa_client_bk" bk

    where  bk.client_bk in  
    ('SBL_21','SBL_1','SBL_11','SBL_31','SBL_41','SBL_111','SBL_51','SBL_231')
