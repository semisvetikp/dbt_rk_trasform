{{ 
    config(materialized='rk_mat',
        target_schema_name = 'dwh_sbx',
        target_table_name = 'psa_order_rk',
        bk_table_name = 'dwh_sbx.psa_order_bk'
    ) 
}}

{%- set bk_tbl = source('dwh_sbx', 'psa_order_bk') -%}

select 
*
from {{ bk_tbl}} bk