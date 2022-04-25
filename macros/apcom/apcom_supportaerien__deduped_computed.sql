{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique (ou spécifique) vers deduped et computed

make it a table IF needed for further processing before
#}

{% macro apcom_supportaerien__deduped_computed(translated_source_relation, fieldPrefix) %}

{% set sourceModel = source_or_test_ref('appuiscommuns', 'osmgeodatamine_powersupports') %}

with id_deduped as (
    -- id deduplication :
    -- OR LATER ON normalized id
    -- FOR MORE PERFORMANCE, REQUIRES PRIMARY KEY ON ID AND A TABLE SO NOT ON SOURCE
    -- OK : 44s rather than 0,44 if on 1m lines rather than the 200 lines, even on translation view (or source view)
    {#{ dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ dedupe(translated_source_relation, id_fields=['"' + fieldPrefix + 'src_id"']) }}

), geometry_deduped as (
    {#{ dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ dedupe('id_deduped', id_fields=['"geometry"']) }}

), computed as (
    {{ apcom_supportaerien_translation__computed("geometry_deduped") }}
)

-- NB. reconciliation to communes requires a geometry field, so can't be done on the source (and is more efficient being in a table)
-- moreover, commune is not necessary for other translation handlings (dedup...). And doing it after translation allows to do it all in one go.

select * from computed
          
{% endmacro %}