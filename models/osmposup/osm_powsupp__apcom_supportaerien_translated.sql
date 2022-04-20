{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie spécifique à la source

make it a table IF needed for further processing before 
#}

{{
  config(
    materialized="table"
  )
}}

{% set fieldPrefix = "appuiscommunssupp__" %}

{% set sourceModel = source_or_test_ref('appuiscommuns', 'osmgeodatamine_powersupports') %}

with translation_specific as (
    {{ osm_powsupp__apcom_supportaerien_specific(sourceModel) }}
    
), id_deduped as (
    -- id deduplication :
    -- OR LATER ON normalized id
    -- FOR MORE PERFORMANCE, REQUIRES PRIMARY KEY ON ID AND A TABLE SO NOT ON SOURCE
    -- OK : 44s rather than 0,44 if on 1m lines rather than the 200 lines, even on translation view (or source view)
    {#{ dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ dedupe('translation_specific', id_fields=['"appuiscommunssupp__src_id"']) }}
    
), geometry_deduped as (
    {#{ dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ dedupe('translation_specific', id_fields=['"geometry"']) }}
    
), computed as (
    {{ apcom_supportaerien_translation__computed("geometry_deduped") }}
)

-- NB. reconciliation to communes requires a geometry field, so can't be done on the source (and is more efficient being in a table)
-- moreover, commune is not necessary for other translation handlings (dedup...). And doing it after translation allows to do it all in one go.

select * from computed
