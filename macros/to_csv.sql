{#
version csv-isable (préparée pour export CSV) :
- geojson-ize geo fields (TODO convert sr if required) ; TODO keep original ?
Superset supports geohash, Polyline, geojson. But the only way Polyline is served on the way is
through geoserver WFS. So the only use of Polyline etc. is here to/from DBT models (read/prepare seeds).
- json-ize SQL arrays
- text-ify everything else besides numeric

parameters :
- source : a dbt model (from ref() or source()) (NOT a WITH-defined alias,
because it is always used in another _csv.sql model). By default is the current
model.name minus the (_wkt)_csv suffix.
- wkt_rather_than_geosjon : for _expected (geojson loses precision) rather than
ckan. GeoJSON is by default because is the most obvious and useful (ckan)
format for CSV.
#}

{% macro to_csv(source=none, wkt_rather_than_geosjon=false) %}

{% set source = source if source else ref(model.name[:(-8 if wkt_rather_than_geosjon else -4)]) %}
{% set cols = adapter.get_columns_in_relation(source) | list %}

select
    {% for col in cols %}
        {% if modules.re.match("geo.*", col.name, modules.re.IGNORECASE) %}
          {% if wkt_rather_than_geosjon %}ST_AsText{% else %}ST_AsGeoJSON{% endif %}({{ source }}.{{ adapter.quote(col.name) }}) as {{ adapter.quote(col.name) }}
        {% elif col.data_type == 'ARRAY' %}
          array_to_json({{ source }}.{{ adapter.quote(col.name) }}) as {{ adapter.quote(col.name) }}
        {% elif col.is_string() or col.is_number() %}
          {{ source }}.{{ adapter.quote(col.name) }}
        {# % elif col.data_type == 'date' % ::text transforms date to rfc3339 by default i.e. 'YYYY-MM-DDTHH24:mi:ss.SSS' #}
        {% else %}
          {{ source }}.{{ adapter.quote(col.name) }}::text
        {% endif %}
        {% if not loop.last %},{% endif %}
        -- TODO NOT IGNORECASE
        -- col.data_type : {{ col.data_type }} ; col.name :  {{ col.name }} ; test : {{ modules.re.match("geo.*", col.name, modules.re.IGNORECASE) }}
    {% endfor %}
    --, '{ "a":1, "b":"zz" }'::json as test
    from {{ source }}

{% endmacro %}