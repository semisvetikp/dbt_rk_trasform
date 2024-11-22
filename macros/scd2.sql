{% materialization scd2_uzi, adapter='oracle' %}
    {% set target_schema_name = generate_schema_name(config.require('schema')) %}
    {% set target_table_name = config.require('alias') %}
    {% set meta_table = config.require('meta_schema_name') + '.' + config.require('meta_table_name') %}
    {% set primary_key_list = config.require('primary_key_list',) %}
    {% set model_database = model.database %}
    {% set date_load = config.require('date_load') %}
    {% set exclude_columns_list = ['LOAD_ID', 'EFFECTIVE_FROM_DTTM', 'EFFECTIVE_TO_DTTM', 'DELETED_FLG'] %}
    {% set dag_id = config.require('dag_id') %}
    {% set task_id = config.require('task_id') %}
    {% set dag_execution_dttm = config.require('dag_execution_dttm') %}
    {% set dag_execution_date = config.require('dag_execution_date') %}
    {% set run_type = config.require('run_type') %}
    {% set af_run_queued_at = config.require('af_run_queued_at') %}
    {% set period_from_date = config.require('period_from_date') %}
    {% set period_to_date = config.require('period_to_date') %}

    {% if model_database == 'None' or model_database is undefined or model_database is none %}
        {% set model_database = get_database_name() %}
    {% endif %}

    {% if not adapter.check_schema_exists(model_database, target_schema_name) %}
        {% do create_schema(model_database, target_schema_name) %}
    {% endif %}

    {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model_database,
          schema=target_schema_name,
          identifier=target_table_name,
          type='table') -%}

    {{ number_of_nulls_val_in_pk(primary_key_list) }}

    {% set build_sql %}
    {% set load_id = get_load_id('DWH_MD', 'TASK_LOAD_STAT_DATA', 'DWH_MD_LOAD_ID_SEQ') %}
        {% if not target_relation_exists %}

            {% call statement('main') %}
                CREATE TABLE {{ target_relation }} as {{ sql }} WHERE 1=0
            {% endcall %}

            {% set drop_column %}
                ALTER TABLE {{ target_relation }}
                    DROP (SNAPSHOT_DATE, DLT_STAT)
            {% endset %}

            {% do run_query(drop_column) %}

            {% set add_column %}
                ALTER TABLE {{target_relation}}
                    ADD (LOAD_ID NUMBER(*,0),
                        EFFECTIVE_FROM_DTTM TIMESTAMP,
                        EFFECTIVE_TO_DTTM TIMESTAMP DEFAULT TO_DATE('5999-01-01', 'YYYY-MM-DD'),
                        DELETED_FLG CHAR (1) DEFAULT '0')
            {% endset %}

            {% do run_query(add_column) %}

            {% set first_insert_rows %}
                INSERT INTO {{ target_relation }}
                            (
                                {{ target_columns(target_relation, exclude_columns_list) }},
                                EFFECTIVE_FROM_DTTM,
                                LOAD_ID
                            )
                    SELECT 
                        {{ target_columns(target_relation, exclude_columns_list) }},
                        SNAPSHOT_DATE,
                        '{{ load_id }}' AS LOAD_ID
                    FROM ({{ sql }})
            {% endset %}

            {% do run_query(first_insert_rows) %}

            {% set first_insert_meta %}
                UPDATE {{ meta_table }}
                    SET 
                        DAG_ID = '{{ var('dag_id') }}',
                        TASK_ID = '{{ var('task_id') }}',
                        DAG_EXECUTION_DTTM = TO_TIMESTAMP('{{ var('dag_execution_dttm') }}', 'YYYY-MM-DD HH24:MI:SS.FF'),
                        AF_RUN_QUEUED_AT = TO_TIMESTAMP('{{ var('af_run_queued_at') }}', 'YYYY-MM-DD HH24:MI:SS.FF'),
                        SOURCE_NAME = 'eais',
                        SUCCESS = 1,
                        TABLE_NAME = '{{ target_relation.table }}',
                        INSERT_ROWS = (SELECT COUNT(*) FROM {{ target_relation }}),
                        UPDATE_ROWS = 0,
                        DELETE_ROWS = 0,
                        SCHEMA_NAME = '{{ target_relation.schema }}'            
                    WHERE LOAD_ID = {{ load_id }}
            {% endset %}

            {% do run_query(first_insert_meta) %}

            {{ adapter.commit() }}
            {{ return({'relations':[target_relation]}) }}
        {% else %}
            {% if date_load == get_max_date(target_relation)|string()|truncate(10, killwords=True, end='') %}
                {{ reload_table (target_relation, exclude_columns_list, date_load, primary_key_list) }}
                {{ delete_row_date(target_relation, date_load) }}
            {% endif %}

            {% set max_date = get_max_date(target_relation) %}

            BEGIN

            {% set inser_rows = isert_rows_with_flg(target_relation, exclude_columns_list, load_id, max_date, type_flg='I') %}
            {{ inser_rows }};

            {% set update_rows = isert_rows_with_flg(target_relation, exclude_columns_list, load_id, max_date, type_flg='U') %}
            {{ update_rows }};

            {% set delete_rows = isert_rows_with_flg(target_relation, exclude_columns_list, load_id, max_date, type_flg='D', deleted_flg=1) %}
            {{ delete_rows }};

            	MERGE INTO {{ target_relation }} t
                USING (
                        SELECT *
                        FROM ( {{ sql }} )
                        WHERE SNAPSHOT_DATE = TO_TIMESTAMP({{ date_load }}, 'YYYY-MM-DD HH24:MI:SS.FF')
                        AND DLT_STAT <> 'I'
                    ) s
                    ON ({% for key in primary_key_list -%}
                            t.{{ key }} = s.{{ key }}
                            {% if not loop.last %}AND{% endif %}
                        {% endfor -%}
                        AND t.EFFECTIVE_FROM_DTTM < s.SNAPSHOT_DATE)
                WHEN MATCHED THEN UPDATE SET 
                    t.EFFECTIVE_TO_DTTM = s.SNAPSHOT_DATE - INTERVAL '1' SECOND
                        WHERE t.EFFECTIVE_TO_DTTM = TO_DATE('5999-01-01', 'YYYY-MM-DD');

                {% set count_rows_insert = count_rows_flg(target_relation, date_load, type_flg='I') %}
                {% set count_rows_update = count_rows_flg(target_relation, date_load, type_flg='U') %}
                {% set count_rows_delete = count_rows_flg(target_relation, date_load, type_flg='D', ) %}

                UPDATE {{ meta_table }}
                
                    SET DAG_ID = '{{ var('dag_id') }}',
                        TASK_ID = '{{ var('task_id') }}',
                        DAG_EXECUTION_DTTM = TO_TIMESTAMP('{{ var('dag_execution_dttm') }}', 'YYYY-MM-DD HH24:MI:SS.FF'),
                        AF_RUN_QUEUED_AT = TO_TIMESTAMP('{{ var('af_run_queued_at') }}', 'YYYY-MM-DD HH24:MI:SS.FF'),
                        SOURCE_NAME = 'eais',
                        SUCCESS = 1,
                        TABLE_NAME = '{{ target_relation.table }}',
                        INSERT_ROWS = ({{ count_rows_insert }}),
                        UPDATE_ROWS = ({{ count_rows_update }}),
                        DELETE_ROWS = ({{ count_rows_delete }}),
                        SCHEMA_NAME = '{{ target_relation.schema }}'
                    WHERE LOAD_ID = {{ load_id }};

            	COMMIT;
            END;
        {% endif %}
    {% endset %}
    {% call statement('main') %}
        {{ build_sql }}
    {% endcall %}
    {{ adapter.commit() }}
    {{ return({'relations':[target_relation]}) }}
{% endmaterialization %}