{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie spécifique à la source
TODO view
#}

{{
  config(
    materialized="table"
  )
}}

with translation_specific as (
    {{ osm_powsupp__apcom_supportaerien_specific(source_or_test_ref('appuiscommuns', 'osmgeodatamine_powersupports')) }}
), computed as (
    {{ apcom_supportaerien_translation__computed("translation_specific") }}
)
select * from computed
