{#
_translated step
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type supportaerien de la source "osmgeodatamine_powersupports"
Partie spécifique à la source

make it a table IF needed for further processing before

OLD 171s with deduped_computed and indexed on Id & geometry

    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'IdSupportAerien"']},
      {'columns': ['geometry'], 'type': 'gist'},]
#}

-- else dbt was unable to infer all dependencies for the model

{% set fieldPrefix = "apcomsup_" %}

{{
  config(
    materialized="view",
  )
}}

{% set src_priority = "0" %}
{% set sourceModel = ref('apcom_src_apcom_osm_parsed') if not var('use_example') else ref('apcom_osm_supportaerien_example_stg') %}

-- only one source, otherwise dedup would have required to index src_name & priority so would have to be in another, downstream model
with imported as (
    select * from {{ sourceModel }}

), translated as (
    {{ osm_powsupp__apcom_supportaerien('imported', src_priority) }}

)
select * from translated
