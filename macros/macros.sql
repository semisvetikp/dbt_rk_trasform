
------------------------------------------------------------------------------------------------

{% macro isert_rows_with_flg(bk_rk_table) %}
    INSERT INTO {{ bk_rk_table }} 
                    (
                        {{ target_columns( target_relation, exclude_columns_list ) }}

                    )
        SELECT 
                {{ target_columns( target_relation, exclude_columns_list ) }}
        FROM ( {{ sql }} )
{%- endmacro %}

------------------------------------------------------------------------------------------------
{# Получение списка  бизнес ключей #}

{% macro get_bk_list (src, bk_rk_tbl, bk_name) %}
    {% set bk_query %}
        select 
            bk.{{ bk_name }}
        from {{ src }} bk
        left  join {{ bk_rk_tbl }} bkrk 
                on  bk.{{bk_name}} = bkrk.{{bk_name}}
        where bkrk.{{bk_name}} is null
    {% endset %}
    {% set stmt = run_query(bk_query) %}
    {% if execute %}
        {% set bk_list = stmt.columns[0].values() %}
    {% else %}
        {% set bk_list = [] %}
    {% endif %}
    {{ return(bk_list) }}
{% endmacro %}

------------------------------------------------------------------------------------------------
{# Получение маски и длины типа бизнес ключа #}

{% macro get_bk_param (bk_type, bk_list) %}
    {% set bk_query %}
        select 
            bk_rx_txt,
            bk_length
        from {{ source ('dwh_sbx', 'etl_bk_type') }}
        where bk_type_cd = '{{ bk_type }}' 
    {% endset %}
    {% set stmt = run_query(bk_query) %}
    {% if execute %}
        {% if stmt.columns[0] %}
            {% set val_bk_rx_txt = stmt.columns[0].values()[0] %}
            {% set val_bk_length = stmt.columns[1].values()[0] %}
        {% else %}
            {{ exceptions.raise_compiler_error("No bk type`s with this name were found.") }}
        {% endif %}
    {% else %}
        {% set bk_list = [] %}
    {% endif %}
    
    {% set re = modules.re %}
    {% set result_bk_list = [] %}
    {% for bk in bk_list %}
        {% if re.match(val_bk_rx_txt, bk, re.IGNORECASE) and bk|length <= val_bk_length %}
            {% do result_bk_list.append(bk) %}
        {% endif %}
    {% endfor %}
    {{ return(result_bk_list) }}
{% endmacro %}


------------------------------------------------------------------------------------------------
{# Проверка входящих значений #}

{% macro validate_input_values (bk_tbl, bk_rk_tbl, bk_type, bk_list) %}
    {% set bk_query %}
        select 
            count(*)
        from {{ source ('dwh_sbx', 'etl_bk_type') }}
        where bk_type_cd = '{{ bk_type }}' 
    {% endset %}
    {% set stmt = run_query(bk_query) %}
    {% if execute %}
        {% if stmt.columns[0].values()[0] == 0 %}
               {{ return(none) }}
        {% endif %}
    {% endif %}
    {{ return(bk_list) }}
{% endmacro %}


