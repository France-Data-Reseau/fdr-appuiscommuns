{#

#}


{% macro to_boolean_or_null(column_name, source) %}
{% set col = adapter.get_columns_in_relation(source) | selectattr("name", "eq", column_name) | list | first %}
{% if not col or col.is_boolean() %}
  {{ source }}.{{ adapter.quote(column_name) }}
{% else %}
  {{ schema }}.to_boolean_or_null({{ source }}.{{ adapter.quote(column_name) }})
{% endif %}
{% endmacro %}