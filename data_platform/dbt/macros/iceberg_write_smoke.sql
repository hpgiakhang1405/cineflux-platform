{% macro iceberg_write_smoke() %}
    {% set smoke_relation = api.Relation.create(
        database=env_var('DBT_ICEBERG_CATALOG'),
        schema=env_var('DBT_GOLD_SCHEMA'),
        identifier='iceberg_write_smoke',
        type='table'
    ) %}

    {% call statement('drop_existing_smoke_table', auto_begin=false) %}
        drop table if exists {{ smoke_relation }}
    {% endcall %}

    {% call statement('create_smoke_table', auto_begin=false) %}
        create table {{ smoke_relation }} as
        select 1 as write_test
    {% endcall %}

    {% call statement('drop_smoke_table', auto_begin=false) %}
        drop table {{ smoke_relation }}
    {% endcall %}

    {{ log('Iceberg write smoke test passed.', info=true) }}
{% endmacro %}
