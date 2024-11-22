{{ 
    config(materialized='rk_mat',
        target_schema_name = 'dwh_sbx',
        target_table_name = 'rk_mat_test',
        bk_list = ['client_bk'],
        bk_type = ['CLIENT'],
        rk_list = ['client_rk'],
        bk_rk_table = ['dwh_sbx.psa_client_bk_rk'],
        bk_table_name = 'dwh_sbx.psa_client_bk'
    ) 
}}

{%- set bk_tbl = source('dwh_sbx', 'psa_client_bk') -%}

select 
*
from {{ bk_tbl}} bk
