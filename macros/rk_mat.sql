{% materialization rk_mat, adapter='default' %}
{% set target_schema_name = generate_schema_name(config.require('schema')) %}
{% set target_table_name = config.get('target_table_name') %}
{% set bk_list = config.get('bk_list') %}	
{% set bk_type = config.get('bk_type') %}	
{% set rk_list = config.get('rk_list') %}	
{% set bk_rk_table = config.get('bk_rk_table') %}	
{% set bk_table_name = config.get('bk_table_name') %}	


{% set target_relation = this -%}
{% set build_sql %}
{% for i in range( bk_list|length) %}
    {%- set bk_valid_list = get_bk_param( bk_type[i] , get_bk_list(bk_table_name, bk_rk_table[i] , bk_list[i])) -%}
       insert into {{ bk_rk_table[i] }} ({{rk_list[i]}}, {{bk_list[i]}}) 
        select 
            md5(BK.{{bk_list[i]}}) ,
            bk.{{ bk_list[i] }}
        from ( {{ sql }} ) bk 
        {% if bk_valid_list|length > 0 %}
            where  bk.{{bk_list[i]}} in  
            (
                {%- for bk in bk_valid_list  -%}
                    '{{ bk }}'
                    {%- if not loop.last %},{% endif -%}
                {%- endfor -%} )
        {% else %}
            where  1=0
        {% endif %}
; {% endfor %} 
{% endset %}
{% do run_query(build_sql) %}

{% set build_sql2 %}
       insert into   {{ target_schema_name }}.{{ target_table_name }} 
            select 
                {{ target_columns(target_relation) }} 
            from ({{ sql}} )bk
            inner  join {{ bk_rk_table[0]  }} bkrk on 
            bk.{{bk_list[0]}} = bkrk.{{bk_list[0]}} 
            {% if bk_list|length > 1 %}
                {% for i in range( bk_list|length - 1) %}
                    left join {{ bk_rk_table[i+1]  }} on 
                        bk.{{bk_list[i+1]}} =  {{ bk_rk_table[i+1] }}.{{bk_list[i+1]}} 
                {% endfor %}
            {% endif %} 
            
{% endset %}

    {% call statement('main') %}
        {{ build_sql2 }}
    {% endcall %}
    {{ adapter.commit() }}
    {{ return({'relations':[target_relation]}) }}
{% endmaterialization %}
