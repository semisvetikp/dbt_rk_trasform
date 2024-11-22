------------------------------------------------------------------------------------------------
--{# Получение списка полей таблицы таргета без полей исключений (например технических) #}
{% macro target_columns_without(target_relation, exclude_columns_list) %}
    {%- set columns = adapter.get_columns_in_relation( target_relation ) -%}
    {% for col in columns if col.column not in exclude_columns_list -%}
            {{col.column}}{% if not loop.last %},{% endif %}
    {% endfor -%}
{% endmacro -%}

------------------------------------------------------------------------------------------------
--{# Получение списка полей таблицы таргета  #}
{% macro target_columns(target_relation) %}
    {%- set columns = adapter.get_columns_in_relation( target_relation ) -%}
    {% for col in columns  -%}
            {{col.column}}{% if not loop.last %},{% endif %}
    {% endfor -%}
{% endmacro -%}