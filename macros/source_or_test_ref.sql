{#
inspired by https://servian.dev/unit-testing-in-dbt-part-1-d0cc20fd189a
#}

{% macro source_or_test_ref(opt_source_name, model_name) %}

      {% if target.name == 'test' %}

            {# if test, it's obligatorily a ref, that has been installed by a dbt run in this project,
            to a model provided by this project or one of its deps
            #}
            {% set test_ref = { 'test_ref' : none} %}
            {% for suffix in var("test_extract_suffixes") %}
              {% if test_ref.test_ref is none %}
                {% if test_ref.update({'test_ref': adapter.get_relation(
                      database = this.database,
                      schema = this.schema,
                      identifier = model_name ~ suffix) })
                %}{% endif %}
                -- macro source_or_test_ref {{ suffix }} get_relation('{{ model_name ~ suffix }}') : {{ test_ref.test_ref }}
              {% else %}
                {{ return(test_ref.test_ref) }}
              {% endif %}
            {% endfor %}

            {% if test_ref is not none %}
              {{ return(test_ref.test_ref) }}
            {% endif %}

      {% endif %}

      {% for relation in dbt_utils.get_relations_by_pattern(this.schema, model_name ~ '_src') %}
          {# else either it's a ref, therefore created in this schema, even if defined by a dep package #}
          {{ return(relation) }}
      {% endfor %}

      {# or a local source TODO and what if remote source ex. ODS communes ? #}
      {{ return(source(opt_source_name, model_name)) }}
 
{% endmacro %}