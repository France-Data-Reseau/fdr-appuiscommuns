{#
TODO from geojson or polyline (to_csv()), json : according to param, example data, meta ?

lecture de version csv-isable (préparée pour export CSV) :
- de geojson-ize geo fields
TODO convert sr if required
TODO handle custom convert (point without POINT) ? NO that's the point of _translated
TODO so help _translated by cols_from_csv() ?
TODO keep original ? Superset supports geohash, Polyline, geojson. But the only
way Polyline is served on the way is through geoserver WFS. So the only use of
Polyline is here to/from DBT models (read/prepare seeds), so rather use geojson
- LATER de json-ize SQL arrays
- convert from text : *_Id to uuid, numbers
- ? textify everything else

parameters :
- source : a dbt model (from ref() or source()), by default the current one minus _stg if any (NOT a WITH-defined alias, because it is always used in another _csv.sql model)
- column_models : used to guide parsing of values from text and add missing columns as NULL if enabled (complete_columns_with_null)
Only the first column with a given name is kept?
- complete_columns_with_null
- wkt_rather_than_geosjon
- date_formats : in the order of parsing preference, by default : 'YYYY-MM-DDTHH24:mi:ss.SSS' (RFC3339), 'YYYY/MM/DD HH24:mi:ss.SSS', 'DD/MM/YYYY HH24:mi:ss.SSS'

optional_column_model_TODO_or_types
#}

{% macro from_csv(source, column_models=none, complete_columns_with_null=false, wkt_rather_than_geosjon=false,
    date_formats=['YYYY-MM-DDTHH24:mi:ss.SSS', 'YYYY/MM/DD HH24:mi:ss.SSS', 'DD/MM/YYYY HH24:mi:ss.SSS'],
    geo_pattern="geo.*", uuid_pattern="_Id|_Ref") %}

{% set source = source if source else ref(model.name | replace('_stg', '')) %}

{%- set cols = adapter.get_columns_in_relation(source) | list -%}
{%- set col_names = cols | map(attribute='name') | list -%}

{%- set all_col_names = [] -%}
{%- set all_def_cols = [] -%}
{% for column_model in column_models %}
  {% for col in adapter.get_columns_in_relation(column_model) | list %}
    {% if col.name not in all_col_names %}
      {% if all_def_cols.append(col) %}{% endif %}
      {% if all_col_names.append(col.name) %}{% endif %}
    {% endif %}
  {% endfor %}
{% endfor %}
{% for col in cols %}
  {% if col.name not in all_col_names %}
    {% if all_def_cols.append(col) %}{% endif %}
    {% if all_col_names.append(col.name) %}{% endif %}
  {% endif %}
{% endfor %}

{%- set def_cols = all_def_cols if complete_columns_with_null else (all_def_cols | selectattr("name", "in", col_names) | list) -%}

select
    {% for def_col in def_cols %}
        {% if def_col.name not in col_names %}
          NULL as {{ adapter.quote(def_col.name) }}
        {% else %}

        -- def_col.data_type : {{ def_col.data_type }} ; def_col.name :  {{ def_col.name }} ; test : {{ modules.re.match("geo.*", def_col.name, modules.re.IGNORECASE) }}

        {% if modules.re.match(geo_pattern, def_col.name, modules.re.IGNORECASE) %}
          {# TODO NOT IGNORECASE #}
          {% if not wkt_rather_than_geosjon %}
          ST_Transform(ST_GeomFromGeoJSON({{ source }}.{{ adapter.quote(def_col.name) }}), 4326) as {{ adapter.quote(def_col.name) }}
          {% else %}
          ST_GeomFROMText({{ source }}.{{ adapter.quote(def_col.name) }}, 4326) as {{ adapter.quote(def_col.name) }}
          {% endif %}
          --NOO ST_PointFromText('POINT(' || replace(c.geo_point_2d, ',', ' ') || ')', 4326) as geo_point_4326,
        {# TODO from json : according to param, example data, meta ?
        {% elif def_col.data_type == 'ARRAY' %}
          array_to_json({{ source }}.{{ adapter.quote(def_col.name) }}) as {{ adapter.quote(def_col.name) } #}
        {% elif modules.re.match(uuid_pattern, def_col.name) %}
          {{ source }}.{{ adapter.quote(def_col.name) }}::uuid
        {% elif def_col.is_number() %}
          {{ to_numeric_or_null(def_col.name, source) }} as {{ adapter.quote(def_col.name) }}
          --{{ schema }}.to_numeric_or_null({{ source }}.{{ adapter.quote(def_col.name) }}) as {{ adapter.quote(def_col.name) }} -- or merely ::numeric ?
          --{{ source }}.{{ adapter.quote(def_col.name) }}::numeric -- NOT to_numeric_or_null else No function matches the given name and argument types.
        {% elif def_col.data_type == 'date' %}
          {{ schema }}.to_date_or_null({{ source }}.{{ adapter.quote(def_col.name) }}::text, {% for fmt in date_formats %}'{{ fmt }}'::text{% if not loop.last %}, {% endif %}{% endfor %}) as {{ adapter.quote(def_col.name) }}
        {% elif def_col.data_type == 'boolean' %}
          {{ to_boolean_or_null(def_col.name, source) }} as {{ adapter.quote(def_col.name) }}
          --{{ schema }}.to_boolean_or_null({{ source }}.{{ adapter.quote(def_col.name) }}) as {{ adapter.quote(def_col.name) }} -- ? allows for 'oui'
          --{{ source }}.{{ adapter.quote(def_col.name) }}
        {% elif def_col.is_string() %}
          {{ source }}.{{ adapter.quote(def_col.name) }}::text -- in case it's NOT text ex. int4 because of dbt seed !
        {% else %}
          {{ source }}.{{ adapter.quote(def_col.name) }}::text
        {% endif %}

        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    --, '{ "a":1, "b":"zz" }'::json as test
    from {{ source }}

{% endmacro %}