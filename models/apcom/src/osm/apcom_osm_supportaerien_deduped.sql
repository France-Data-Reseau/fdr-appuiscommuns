{#
Actually dedup is done in _translated because before _computed
If with additional index on _2154 and _translated not a table, both still take less time

Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique

is a table only if has reconciliation
69s with its own indexes (23s without) ; 40s

applies exact dedup by src_id and geometry

  config(
    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'IdSupportAerien"']},
      {'columns': order_by_fields},
      {'columns': ['geometry'], 'type': 'gist'},
      {'columns': ['geometry_2154'], 'type': 'gist'},]
#}

{% set fieldPrefix = "apcomsup_" %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="view",
  )
}}

{% set source_model = ref("apcom_osm_supportaerien_translated") %}

with translated as (
    select * from {{ source_model }}

), id_deduped as (
    -- id deduplication :
    -- OR LATER ON normalized id
    -- FOR MORE PERFORMANCE, REQUIRES PRIMARY KEY ON ID AND A TABLE SO NOT ON SOURCE
    -- OK : 44s rather than 0,44 if on 1m lines rather than the 200 lines, even on translation view (or source view)
    {#{ dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ fdr_francedatareseau.dedupe("translated", id_fields=['"' + fieldPrefix + 'src_id"']) }}

), geometry_deduped as (
    {#{ fdr_francedatareseau.dedupe('"' + this.schema + '"."osmgeodatamine_powersupports"', id_fields=['"osm_id"']) }#}
    {{ fdr_francedatareseau.dedupe('id_deduped', id_fields=['"geometry"']) }}

)
select
    {{ dbt_utils.star(source_model, except=["apcomsup_com_code", "apcomsup_com_name"]) }}
from geometry_deduped
order by "{{ order_by_fields | join('" asc, "') }}" asc