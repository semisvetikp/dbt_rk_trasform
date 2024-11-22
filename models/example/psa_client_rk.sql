
-- Use the `ref` function to select from other models
{{ config(materialized='table') }}

{%- set bk_tbl = source('dwh_sbx', 'psa_client_bk') -%}
{%- set bk_rk_tbl = source('dwh_sbx', 'psa_client_bk_rk') -%}
{%- set bk_name = 'client_bk' -%}

select 
src_system_cd,
client_rk,
client_fio
from {{ bk_tbl}} bk
inner  join {{ bk_rk_tbl }} bkrk on 
bk.{{bk_name}} = bkrk.{{bk_name}} 