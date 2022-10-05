{#
DESACTIVE par défaut, gardé à titre d'exemple de array_linked

Liaison aux communes des données normalisées de toutes les sources de type appuiscommuns.supportaerien,
en créant la table de ces supports enrichis ("in place") de leurs communes (en colonne ARRAY, indexée pour exploitation
performante).

Une approche davantage normalisée est plus standard que la présente en base de données relationnelle (mais pas en NoSQL), i.e.
avec une table intermédiaire pour la relation n-n qui d'ailleurs amène beaucoup moins de données (et d'index)
dupliquées. Et qu'avec les seuls champs de la définition ça prenne 2 fois moins de temps le confirme.

1 pass linking array enrichment of apcomsup by com

TODO doc __arr gin
apcomsup_fdrcom_insee_id__arr

900s => 340s avec les seuls champs de la definition
OLD 176.45s.
#}

{% set fieldPrefix = 'apcomsup_' %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    enabled=var("enableArrayLinked", false) | as_bool,
    materialized="table",
    indexes=[
      {'columns': ['apcomsup_com_code__arr'], 'type': 'gin'},
      {'columns': ['"' + fieldPrefix + 'IdSupportAerien"']},
      {'columns': order_by_fields},
      {'columns': ['geometry'], 'type': 'gist'},
    ]
  )
}}

-- TODO {% set sourceModel = ref('apcom_std_supportaerien_deduped') %}
{% set sourceModel = ref('apcom_std_supportaerien_unified') %}

with apcomsup as (
    select * from {{ sourceModel }}

), commune_linked as (
    -- reconciliation :
    -- NB. reconciliation to communes requires a geometry field, so can't be done on the source (and is more efficient being in a table)
    -- moreover, commune is not necessary for other translation handlings (dedup...). And doing it after translation allows to do it all in one go.
    -- TODO remove
    {%- set fields = adapter.get_columns_in_relation(ref("apcom_def_supportaerien_definition")) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
    {# OLD %- set fields = ['_dbt_source_relation', 'apcomsup_src_priority'] + adapter.get_columns_in_relation(ref('apcom_def_supportaerien_definition')) | map(attribute="name") | list -%#}-- BEWARE without | list it stays a generator that can only be iterated once
    -- (no need to except=[apcomsup_com_code"] because in the ex. osm source it is osmposup_com_code)
    {# OLD % set cols = dbt_utils.star(sourceModel).split(',') %#}
    {{ apcom_supportaerien_array_link_geometry_commune("apcomsup", id_field=fieldPrefix + "IdSupportAerien", fields=fields) }}

)

select * from commune_linked