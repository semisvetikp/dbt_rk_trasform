
{{ 
    config(materialized='rk_mat',
        target_schema_name = 'dwh_sbx',
        target_table_name = 'psa_client_rk',
        bk_table_name = 'dwh_sbx.psa_client_bk'
    ) 
}}

{%- set bk_tbl = source('dwh_sbx', 'psa_client_bk') -%}

select 
*
from {{ bk_tbl}} bk
