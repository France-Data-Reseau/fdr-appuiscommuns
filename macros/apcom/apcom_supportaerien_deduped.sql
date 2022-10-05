{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique (ou spécifique) vers deduped et computed

make it a table IF needed for further processing before

89s with no index (!)
#}

{% macro apcom_supportaerien_deduped(translated_source_relation, fieldPrefix) %}

with id_deduped as (
    -- id deduplication :
    -- OR LATER ON normalized id
    -- FOR MORE PERFORMANCE, REQUIRES PRIMARY KEY ON ID AND A TABLE SO NOT ON SOURCE
    -- OK : 44s rather than 0,44 if on 1m lines rather than the 200 lines, even on translation view (or source view)
    {#{ dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ fdr_francedatareseau.dedupe("translated_source_relation", id_fields=['"' + fieldPrefix + 'src_id"']) }}

), geometry_deduped as (
    {#{ fdr_francedatareseau.dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ fdr_francedatareseau.dedupe('id_deduped', id_fields=['"geometry"']) }}

)

-- NB. reconciliation to communes requires a geometry field, so can't be done on the source (and is more efficient being in a table)
-- moreover, commune is not necessary for other translation handlings (dedup...). And doing it after translation allows to do it all in one go.

select * from geometry_deduped
          
{% endmacro %}