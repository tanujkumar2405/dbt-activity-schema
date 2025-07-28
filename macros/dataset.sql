{% macro dataset(activity_stream, primary_activity, appended_activities=[]) %}
  {{ return(adapter.dispatch("dataset", "dbt_activity_schema")(activity_stream, primary_activity, appended_activities)) }}
{% endmacro %}

{% macro default__dataset(activity_stream, primary_activity, appended_activities) %}

{% set columns = dbt_activity_schema.columns() %}
{% set primary = dbt_activity_schema.primary %}
{% set appended = dbt_activity_schema.appended %}
{% set alias_cte = dbt_activity_schema.alias_cte %}
{% set alias_column = dbt_activity_schema.alias_column %}
{% set alias_appended_activity = dbt_activity_schema.alias_appended_activity %}
{% set render_join = dbt_activity_schema.render_additional_join_condition %}
{% set render_agg = dbt_activity_schema.render_aggregation %}

with

filter_activity_stream_using_primary_activity as (
    select
        {% for col in primary_activity.included_columns + primary_activity.required_columns %}
        {{ dbt_activity_schema.parse_column(primary(), col) }} as {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from {{ activity_stream }} as {{ primary() }}
    where {{ primary() }}.{{ columns.activity }} = {{ dbt.string_literal(primary_activity.name) }}
      and {{ primary_activity.relationship.where_clause }}
),

{% for activity in appended_activities %}
{% set i = loop.index %}
{{ alias_cte(activity, i) }} as (
    select
        {% for col in primary_activity.included_columns + primary_activity.required_columns %}
        {{ primary() }}.{{ col }},{% endfor %}

        {% for col in activity.included_columns %}
        {% set parsed_col = dbt_activity_schema.parse_column(appended(), col) %}
        {% call activity.relationship.aggregation_func() %}
        {{ parsed_col }}
        {% endcall %} as {{ alias_appended_activity(activity, col) }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from filter_activity_stream_using_primary_activity as {{ primary() }}

    left join {{ activity_stream }} as {{ appended() }}
      on (
        {{ appended() }}.{{ columns.customer }} = {{ primary() }}.{{ columns.customer }}
        and {{ appended() }}.{{ columns.activity }} = {{ dbt.string_literal(activity.name) }}
        and (
          {% if activity.relationship.name == "nth_ever" %}
            {{ activity.relationship.join_clause(activity.relationship.nth_occurance) }}
          {% else %}
            {{ activity.relationship.join_clause() }}
          {% endif %}
        )
        and ({{ activity.additional_join_condition }})
      )

    group by
      {% for col in primary_activity.included_columns + primary_activity.required_columns %}
      {{ primary() }}.{{ col }}{% if not loop.last %},{% endif %}
      {% endfor %}
),
{% endfor %}

rejoin_aggregated_activities as (
    select
      {% for col in primary_activity.included_columns %}
      {{ primary() }}.{{ col }}{% if not loop.last or appended_activities %},{% endif %}
      {% endfor %}

      {% for activity in appended_activities %}
      {% set i = loop.index %}
      {% set last_outer_loop = loop.last %}
      {% for col in activity.included_columns %}
      {{ alias_cte(activity, i) }}.{{ alias_appended_activity(activity, col) }}{% if not (last_outer_loop and loop.last) %},{% endif %}
      {% endfor %}
      {% endfor %}

    from filter_activity_stream_using_primary_activity as {{ primary() }}

    {% for activity in appended_activities %}
    {% set i = loop.index %}
    left join {{ alias_cte(activity, i) }}
      on {{ alias_cte(activity, i) }}.{{ columns.activity_id }} = {{ primary() }}.{{ columns.activity_id }}
    {% endfor %}
)

select * from rejoin_aggregated_activities

{% endmacro %}
