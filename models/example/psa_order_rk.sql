{{ 
    config(materialized='rk_mat',
        target_schema_name = 'dwh_sbx',
        target_table_name = 'psa_order_rk',
        bk_list = ['order_bk', 'client_bk'],
        bk_type = ['ORDER', 'CLIENT'],
        rk_list = ['order_rk', 'client_rk'],
        bk_rk_table = ['dwh_sbx.psa_order_bk_rk', 'dwh_sbx.psa_client_bk_rk'],
        bk_table_name = 'dwh_sbx.psa_order_bk'
    ) 
}}

{%- set bk_tbl = source('dwh_sbx', 'psa_order_bk') -%}

select 
*
from {{ bk_tbl}} bk