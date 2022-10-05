{#
TODO rename _geo_near_dedupe_candidates

2 phase dedup - phase 1 : produce candidates / duplicates ; so mMaterialized as table.

BEWARE using 2 order_by_fields is way too long : [fieldPrefix + "src_priority", fieldPrefix + "src_id"]

    enabled=false,
    indexes=[{'columns': ['"' + fieldPrefix + 'IdSupportAerien"']},
      {'columns': order_by_fields},
      {'columns': ['geometry_2154'], 'type': 'gist'},]
#}

{{
  config(
    materialized="table",
  )
}}

{% set fieldPrefix = 'apcomsup' + '_' %}

{# ['_dbt_source_relation''apcomsup_src_relation'] + #}
{%- set fields = adapter.get_columns_in_relation(ref('apcom_def_supportaerien_definition')) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
-- {{ fields }}

{% set distance_m = 20 %}
{% set criteria = "ST_Distance(ST_Transform(earlier.geometry, 3857), ST_Transform(later.geometry, 3857)) < " ~ distance_m ~ " -- s ; requires transform because 4326 distance is in degrees ; assuming geometry is not NULL" %}
{% set criteria = "ST_DWithin(ST_Transform(earlier.geometry, 3857), ST_Transform(later.geometry, 3857), " ~ distance_m ~ ") -- 143s ; requires transform because 4326 distance is in degrees ; assuming geometry is not NULL" %}
{% set criteria = "ST_DWithin(earlier.geometry_2154, later.geometry_2154, " ~ distance_m ~ ") -- 108s ; Lambert 93 BUT wrong ex. in DOM TOM " %}
{#{ apcom_supportaerien_translation__dedupe_geometry('apcom_std_supportaerien_unified', fieldPrefix + "IdSupportAerien", [fieldPrefix + "src_priority"], fields, criteria) }#}
-- TODO apcom_supportaerien_translation__dup_geometry : first step producing only duplicates,
-- that can be merged according to the expert choices afterwards (rather than static rules)

{% set normalized_source_model_name = 'apcom_std_supportaerien_unified' %}
{#% set normalized_source_model_name = 'apcom_std_supportaerien_commune_linked' %#}
{% set id_field = fieldPrefix + "IdSupportAerien" %}
{% set order_by_fields = [fieldPrefix + "src_priority"] %}
{{ apcom_supportaerien_dedupe_geometry_candidates(normalized_source_model_name, id_field, order_by_fields, fields, criteria) }}