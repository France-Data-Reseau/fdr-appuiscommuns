{#
2 phase dedup - single phase

or osm_powsupp__apcom_supportaerien_translated
#}

{{
  config(
    materialized="view"
  )
}}

{% set fieldPrefix = 'appuiscommunssupp' + '__' %}

{%- set fields = ['_dbt_source_relation'] + adapter.get_columns_in_relation(ref('appuiscommuns_supportaerien__definition')) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once

{{ apcom_supportaerien_translation__dup_geometry('appuiscommuns_supportaerien', fieldPrefix + "Id", fields, [fieldPrefix + "src_name", fieldPrefix + "src_id"], 20) }}
