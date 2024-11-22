{% materialization rk_mat, adapter='default' %}
{% set target_schema_name = generate_schema_name(config.require('schema')) %}
{% set target_table_name = config.require('target_table_name') %}
{% set bk_table_name = config.require('bk_table_name') %}	

{%- set rk_efw = source('dwh_sbx', 'efw_rk_table') -%}
{% set target_relation = this -%}


--{# Проверка существования таблицы таргета #}

--{# Проверка существования таблиц бк_рк #}

--{# Проверка корректности именований бк-полей относительно таблиц бк_рк #}



--{# Поиск в метаданных бк-рк параметры основной сущности #}
{% set bk_main_column_name = [] %}
{% set rk_main_column_name =  [] %}
{% set bk_rk_main_table_name = [] %}

{% set rk_main_param_query %}
select 
    bk_column_name,
    rk_columns_name,
    bk_rk_table_name
from {{ rk_efw }}
where   1=1
        and bk_rk_table_name is not null 
        and rk_columns_is_pk_flg  = 'Y'
        and rk_table_name = '{{ target_schema_name }}.{{ target_table_name }}'
{% endset %}
{% set stmt = run_query(rk_main_param_query) %}
    {% if execute and stmt.columns[2].values() %}
        {% set bk_main_column_name = stmt.columns[0].values()[0] %}
        {% set rk_main_column_name =  stmt.columns[1].values()[0] %}
        {% set bk_rk_main_table_name = stmt.columns[2].values()[0] %}
    {% endif %}

{# Поиск в метаданных бк-рк параметры сущностей внешних ключей #}
{% set rk_fk_params_list = [] %}	
{% set rk_fk_param_query %}
select 
    bk_column_name,
    rk_columns_name,
    bk_rk_table_name
from {{ rk_efw }}
where 1=1 
    and bk_rk_table_name is not null
    and rk_columns_is_fk_flg  = 'Y'
    and rk_table_name = '{{ target_schema_name }}.{{ target_table_name }}'
{% endset %}
{% set stmt = run_query(rk_fk_param_query) %}
    {% if execute and stmt.columns[2].values()%}
        {% set rk_param_result_rows = stmt.rows %}
        {% for row_i in rk_param_result_rows %}
            {% do rk_fk_params_list.append(row_i)  %}	
        {% endfor %}
    {% endif %}

{% if bk_rk_main_table_name|length > 0 %}

    {# Генерация суррогатных ключей  в основной сущности #}
    {% set build_rk_sql %}
    insert into {{ bk_rk_main_table_name}} ({{rk_main_column_name }}, {{bk_main_column_name}}) 
    select 
        md5(BK.{{bk_main_column_name }}) ,
        bk.{{ bk_main_column_name }}
    from ( {{ sql }} ) bk 
    left  join {{ bk_rk_main_table_name }} bkrk 
            on  bk.{{bk_main_column_name}} = bkrk.{{bk_main_column_name}}
    where bkrk.{{bk_main_column_name}} is null
    {% endset %}
    {% do run_query(build_rk_sql) %}

    {# Генерация суррогатных ключей в сущностях внешних ключей #}
    {% if rk_fk_params_list|length > 0 %}
        {% set build_fk_sql %}
        {% for i in range( rk_fk_params_list|length) %}
            insert into {{ rk_fk_params_list[i][2] }} ({{rk_fk_params_list[i][1] }}, {{rk_fk_params_list[i][0] }}) 
                select 
                    md5(BK.{{rk_fk_params_list[i][0] }}) ,
                    bk.{{ rk_fk_params_list[i][0]  }}
                from ( {{ sql }} ) bk 
                left  join {{ rk_fk_params_list[i][2] }} bkrk 
                        on  bk.{{rk_fk_params_list[i][0]}} = bkrk.{{rk_fk_params_list[i][0]}}
                where bkrk.{{rk_fk_params_list[i][0]}} is null
        ; {% endfor %} 
        {% endset %}
        {% do run_query(build_fk_sql) %}
    {% endif %} 

    {# Формирование итоговой rk таблицы #}
    {% set build_final_sql %}
        truncate table {{ target_schema_name }}.{{ target_table_name }} ;
        insert into   {{ target_schema_name }}.{{ target_table_name }} 
                select 
                    {{ target_columns(target_relation) }} 
                from ({{ sql}} )bk
                inner  join {{ bk_rk_main_table_name  }} bkrk on 
                bk.{{bk_main_column_name }} = bkrk.{{bk_main_column_name }} 
                {% if rk_fk_params_list|length > 0 %}
                    {% for i in range( rk_fk_params_list|length ) %}
                        left join {{ rk_fk_params_list[i][2]  }} on 
                            bk.{{rk_fk_params_list[i][0] }} =  {{ rk_fk_params_list[i][2]  }}.{{rk_fk_params_list[i][0] }} 
                    {% endfor %}
                {% endif %} 
                
    {% endset %}
{% else %} 
    {% set build_final_sql %}
        truncate table {{ target_schema_name }}.{{ target_table_name }} ;
        insert into   {{ target_schema_name }}.{{ target_table_name }} 
                select 
                    *
                from ({{ sql}} )bk
                where 1=0
                
    {% endset %}

{% endif %} 

    {% call statement('main') %}
        {{ build_final_sql }}
    {% endcall %}
    {{ adapter.commit() }}
    {{ return({'relations':[target_relation]}) }}
{% endmaterialization %}