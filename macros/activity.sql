{% macro activity(
    relationship,
    activity_name,
    included_columns=var("included_columns", var("dbt_activity_schema", {}).get("included_columns", dbt_activity_schema.columns().values() | list)),
    additional_join_condition="true"
) %}
    {{ return(adapter.dispatch("activity", "dbt_activity_schema")(
        relationship,
        activity_name,
        included_columns,
        additional_join_condition
    )) }}
{% endmacro %}

{% macro default__activity(
    relationship,
    activity_name,
    included_columns,
    additional_join_condition
) %}
    {# Macro for defining a single activity in the dataset. #}

    {# Column mapping from utility macro #}
    {% set columns = dbt_activity_schema.columns() %}

    {# Required columns for join integrity #}
    {% set required_columns = [
        columns.activity_id,
        columns.activity,
        columns.ts,
        columns.customer,
        columns.activity_occurrence,
        columns.activity_repeated_at
    ] %}

    {# Remove any already included columns from required_columns #}
    {% for col in included_columns %}
        {% if col in required_columns %}
            {% do required_columns.remove(col) %}
        {% endif %}
    {% endfor %}

    {# Return a namespace containing all necessary metadata #}
    {% do return(namespace(
        name = activity_name,
        included_columns = included_columns,
        required_columns = required_columns,
        relationship = relationship,
        additional_join_condition = additional_join_condition
    )) %}
{% endmacro %}
