{% set columns_name = [] %}
{%- set target_relation = source('dwh_sbx', 'psa_client_bk_rk') -%}
{%- set columns =  adapter.get_columns_in_relation( target_relation ) -%}
{% for col in columns %}
    {% do columns_name.append(col.column)%}
    {% if not loop.last %},{% endif %}
{% endfor -%}



SELECT 
{% for col in columns %}
     {{col.column}}
     {% if not loop.last %},{% endif %}    
{% endfor -%}
FROM {{ source('dwh_sbx', 'psa_client_bk_rk') }}