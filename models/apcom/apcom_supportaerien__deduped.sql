{#
2 phase dedup - single phase

or osm_powsupp__apcom_supportaerien_translated

TODO copy / separate to _duplicate_geometry
#}

{{
  config(
    materialized="table"
  )
}}

{% set fieldPrefix = 'appuiscommunssupp' + '__' %}

{%- set fields = ['_dbt_source_relation'] + adapter.get_columns_in_relation(ref('appuiscommuns_supportaerien__definition')) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once

{% set distance_m = 20 %}
{% set criteria = "ST_Distance(ST_Transform(earlier.geometry, 3857), ST_Transform(later.geometry, 3857)) < " ~ distance_m ~ " -- requires transform because 4326 distance is in degrees ; assuming geometry is not NULL" %}
{{ apcom_supportaerien_translation__dup_geometry('appuiscommuns_supportaerien', fieldPrefix + "Id", [fieldPrefix + "src_name", fieldPrefix + "src_id"], fields, criteria) }}
