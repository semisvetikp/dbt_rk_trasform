{{ config(materialized='incremental') }}

{%- set bk_tbl = source('dwh_sbx', 'psa_client_bk') -%}
{%- set bk_rk_tbl = source('dwh_sbx', 'psa_client_bk_rk') -%}
{%- set bk_type = 'CLIENT' -%}
{%- set bk_name = 'client_bk' -%}
{%- set bk_list = get_bk_param( bk_type , get_bk_list(bk_tbl, bk_rk_tbl , bk_name)) -%}


select 
md5(BK.{{bk_name}}) as client_rk,
BK.{{bk_name}}
from  {{ bk_tbl }} bk
{% if bk_list|length > 0 %}
    where  bk.{{bk_name}} in  
    (
        {%- for bk in bk_list  -%}
            '{{ bk }}'
            {%- if not loop.last %},{% endif -%}
        {%- endfor -%} )
{% else %}
    where  1=0
{% endif %}