{% test cineflux_unique_combination(model, columns) %}
select
    {%- for column in columns %}
    {{ column }}{{ ',' if not loop.last }}
    {%- endfor %}
from {{ model }}
group by
    {%- for column in columns %}
    {{ column }}{{ ',' if not loop.last }}
    {%- endfor %}
having count(*) > 1
{% endtest %}

{% test cineflux_scd2_one_current(model, natural_key) %}
select {{ natural_key }}
from {{ model }}
group by {{ natural_key }}
having count_if(is_current) <> 1
{% endtest %}

{% test cineflux_scd2_valid_ranges(model) %}
select *
from {{ model }}
where valid_from_ts >= valid_to_ts
{% endtest %}

{% test cineflux_scd2_contiguous(model, natural_key) %}
with ordered as (
    select
        {{ natural_key }},
        valid_from_ts,
        valid_to_ts,
        is_current,
        lead(valid_from_ts) over (
            partition by {{ natural_key }}
            order by valid_from_ts
        ) as next_valid_from_ts
    from {{ model }}
)
select *
from ordered
where
    (next_valid_from_ts is not null and valid_to_ts <> next_valid_from_ts)
    or (next_valid_from_ts is null and not is_current)
    or (next_valid_from_ts is not null and is_current)
{% endtest %}

{% test cineflux_scd2_changes_only(model, natural_key) %}
with ordered as (
    select
        {{ natural_key }},
        valid_from_ts,
        record_hash,
        lag(record_hash) over (
            partition by {{ natural_key }}
            order by valid_from_ts
        ) as previous_record_hash
    from {{ model }}
)
select *
from ordered
where record_hash = previous_record_hash
{% endtest %}

{% test cineflux_feature_timestamp_columns(model) %}
with feature_columns as (
    select
        lower(column_name) as column_name,
        lower(data_type) as data_type
    from {{ model.database }}.information_schema.columns
    where table_schema = '{{ model.schema }}'
      and table_name = '{{ model.identifier }}'
), validation as (
    select
        count_if(column_name = 'event_timestamp') as event_timestamp_count,
        count_if(column_name = 'created') as created_count,
        count_if(column_name = 'created_timestamp') as created_timestamp_count,
        count_if(data_type like 'timestamp%') as timestamp_column_count
    from feature_columns
)
select *
from validation
where event_timestamp_count <> 1
   or created_count <> 1
   or created_timestamp_count <> 0
   or timestamp_column_count <> 2
{% endtest %}

{% test cineflux_gold_forbids_created(model) %}
select column_name
from {{ model.database }}.information_schema.columns
where table_schema = '{{ model.schema }}'
  and table_name = '{{ model.identifier }}'
  and lower(column_name) = 'created'
{% endtest %}

{% test cineflux_created_not_before_event(model) %}
select *
from {{ model }}
where created < event_timestamp
{% endtest %}

{% test cineflux_nonnegative(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}

{% test cineflux_between(model, column_name, minimum, maximum) %}
select *
from {{ model }}
where {{ column_name }} < {{ minimum }}
   or {{ column_name }} > {{ maximum }}
{% endtest %}
