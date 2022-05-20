{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type supportaerien de la source "osmgeodatamine_powersupports"
Partie spécifique à la source

make it a table IF needed for further processing before

171s with deduped_computed and indexed on Id & geometry

    indexes=[{'columns': ['"' + fieldPrefix + 'Id"']},
      {'columns': ['geometry'], 'type': 'gist'},]
#}

-- else dbt was unable to infer all dependencies for the model

{% set fieldPrefix = "apcomsup_" %}

{{
  config(
    materialized="table",
  )
}}

{% set src_priority = "0" %}
{% set sourceModel = source_or_test_ref('appuiscommuns', 'apcom_osm_supportaerien') %} -- TODO sourceOrTestRef(

-- only one source, otherwise dedup would have required to index src_name & priority so would have to be in another, downstream model
with translated as (
    {{ osm_powsupp__apcom_supportaerien(sourceModel, src_priority) }}
), deduped_computed as (
    -- id & geo exact dedup WITHIN geodatamine version of OSM data
    {{ apcom_supportaerien__deduped_computed('translated', fieldPrefix) }}
)
select * from deduped_computed
