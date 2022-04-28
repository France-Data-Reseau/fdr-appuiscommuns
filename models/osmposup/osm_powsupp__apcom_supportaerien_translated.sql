{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type supportaerien de la source "osmgeodatamine_powersupports"
Partie spécifique à la source

make it a table IF needed for further processing before 
#}

-- depends_on: {{ ref('l_appuisaeriens_materiau__osmgeodatamine') }}
-- else dbt was unable to infer all dependencies for the model

{{
  config(
    materialized="table"
  )
}}

{% set fieldPrefix = "appuiscommunssupp__" %}

{% set sourceModel = source_or_test_ref('appuiscommuns', 'osmgeodatamine_powersupports') %}

with translated as (
    {{ osm_powsupp__apcom_supportaerien(sourceModel) }}
), deduped_computed as (
    {{ apcom_supportaerien__deduped_computed('translated', fieldPrefix) }}
)
select * from deduped_computed
