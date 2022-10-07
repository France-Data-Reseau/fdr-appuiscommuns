{#
TODO rename _geo_near_dedupe_candidates !

2 phase dedup - phase 1 : produce candidates / duplicates,
materialized as DBT incremental (filtered on later.last_changed)

BEWARE using 2 order_by_fields is way too long : [fieldPrefix + "src_priority", fieldPrefix + "src_id"]

    enabled=false,
    indexes=[
      {'columns': ['"' + fieldPrefix + 'id"']},
      {'columns': order_by_fields},
      {'columns': ['geometry_2154'], 'type': 'gist'},
    ]
#}

{% set fieldPrefix = 'apcomsup' + '_' %}

{{
  config(
    materialized="incremental",
    unique_key=['"earlier' + fieldPrefix + "id" + '"', '"later' + fieldPrefix + "id" + '"'],
    tags=["incremental", "long"],
  )
}}

{% set distance_m = 20 %}
{% set criteria = "ST_Distance(ST_Transform(earlier.geometry, 3857), ST_Transform(later.geometry, 3857)) < " ~ distance_m ~ " -- s ; requires transform because 4326 distance is in degrees ; assuming geometry is not NULL" %}
{% set criteria = "ST_DWithin(ST_Transform(earlier.geometry, 3857), ST_Transform(later.geometry, 3857), " ~ distance_m ~ ") -- 143s ; requires transform because 4326 distance is in degrees ; assuming geometry is not NULL" %}
{% set criteria = "ST_DWithin(earlier.geometry_2154, later.geometry_2154, " ~ distance_m ~ ") -- 108s ; Lambert 93 BUT wrong ex. in DOM TOM " %}
{% set normalized_source_model_name = 'apcom_std_supportaerien_unified' %}
{% set id_field = fieldPrefix + "id" %}
{% set order_by_fields = [fieldPrefix + "src_priority"] %}
{{ apcom_supportaerien_dedupe_geometry_candidates(normalized_source_model_name, id_field, order_by_fields, 'geometry', criteria) }}