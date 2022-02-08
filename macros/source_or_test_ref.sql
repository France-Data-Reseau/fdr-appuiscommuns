{#
inspired by https://servian.dev/unit-testing-in-dbt-part-1-d0cc20fd189a
#}
{% macro source_or_test_ref(opt_source_name, model_name) %}

      {% if target.name == 'test' %}

            {# if test, it's obligatorily a ref, that has been installed by a dbt run in this project,
            to a model provided by this project or one of its deps
            #}
            {%- set test_ref = adapter.get_relation(
                  database = this.database,
                  schema = this.schema,
                  identifier = model_name ~ '_extract') 
            -%}
            {{ return(test_ref) }}

      {% else %}
      
            {% for relation in dbt_utils.get_relations_by_pattern(this.schema, model_name) %}
                {# else either it's a ref, therefore created in this schema, even if defined by a dep package #}
                {{ return(relation) }}
            {% endfor %}
            
            {# or a local source TODO and what if remote source ex. ODS communes ? #}
            {{ return(source(opt_source_name, model_name)) }}
      
      {% endif %}
 
{% endmacro %}