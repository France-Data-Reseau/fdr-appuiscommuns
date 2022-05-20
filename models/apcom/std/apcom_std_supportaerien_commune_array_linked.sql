{#
Liaison aux communes des données normalisées de toutes les sources de type appuiscommuns.supportaerien.

1 pass linking array enrichment of apcomsup by com

so is a table to store its results

TODO doc __arr gin
apcomsup_fdrcom_insee_id__arr

176.45s.
#}

{% set fieldPrefix = 'apcomsup_' %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'Id"']},
      {'columns': order_by_fields},
      {'columns': ['apcomsup_com_code__arr'], 'type': 'gin'},
      {'columns': ['geometry'], 'type': 'gist'},]
  )
}}

{% set sourceModel = ref('apcom_std_supportaerien_unified') %}

with apcomsup as (
    select * from {{ sourceModel }}

), commune_linked as (
    -- reconciliation :
    -- NB. reconciliation to communes requires a geometry field, so can't be done on the source (and is more efficient being in a table)
    -- moreover, commune is not necessary for other translation handlings (dedup...). And doing it after translation allows to do it all in one go.
    -- TODO remove
    {%- set fields = adapter.get_columns_in_relation(sourceModel) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
    {# OLD %- set fields = ['_dbt_source_relation', 'apcomsup_src_priority'] + adapter.get_columns_in_relation(ref('apcom_def_supportaerien_definition')) | map(attribute="name") | list -%#}-- BEWARE without | list it stays a generator that can only be iterated once
    -- (no need to except=[apcomsup_com_code"] because in the ex. osm source it is osmposup_com_code)
    {# OLD % set cols = dbt_utils.star(sourceModel).split(',') %#}
    {{ apcom_supportaerien_translation__link_geometry_fdrcommune("apcomsup", id_field=fieldPrefix + "Id", fields=fields) }}

)

select * from commune_linked