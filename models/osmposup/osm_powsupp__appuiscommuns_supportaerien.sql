{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique

is a table only if has reconciliation
69s with its own indexes (23s without)
#}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"appuiscommunssupp__Id"']}, 
      {'columns': ['geometry'], 'type': 'gist'},]
  )
}}

with reconciled as (

select * from {{ ref("osm_powsupp__apcom_supportaerien_translated") }}
{# NO else ARRAY fields that can't be added (cast(null as ARRAY) syntax error in _definition when unioning in support_aerien later
{{ apcom_supportaerien_translated__reconciled("osm_powsupp__apcom_supportaerien_translated") }}
#}

{#
), commune_linked as (
    -- reconciliation :
    -- rather doing it after translation, because allows to do it all in one go,
    -- moreover, commune is not necessary for other translation processings (dedup...).
    -- NB. couldn't be done earlier because reconciliation to communes requires a geometry field, so can't be done on the source which hasn't it (and is more efficient being in a table)
    {%- set fields = adapter.get_columns_in_relation(ref('osm_powsupp__apcom_supportaerien_translated')) | map(attribute="column") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
    {#% set cols = dbt_utils.star(sourceModel, except=[fieldPrefix + "fdrcommune__insee_id",
      fieldPrefix + "commune__insee_id", "fdrcommune__insee_id"]).split(',') %}
    {{ apcom_supportaerien_translation__link_geometry_fdrcommune("all", id_field="appuiscommunssupp__Id", fields=fields) }#+}
#}
)

select * from reconciled

{# TODO 2 phase dedup :
select * from {{ apcom_supportaerien_translated__reconciled("osm_powsupp__apcom_supportaerien_translation") }} translation
where translation."{{ fieldPrefix }}Id" not in (select "{{ fieldPrefix }}Id" from {{ ref("apcom_supportaerien_translation__link_geometry_fdrcommune") }})

union

select translation.* from {{ apcom_supportaerien_translated__reconciled("osm_powsupp__apcom_supportaerien_translation") }} translation
join {{ ref("apcom_supportaerien_translation__link_geometry_fdrcommune") }} link on translation."{{ fieldPrefix }}Id" = link."{{ fieldPrefix }}Id" -- NOT left join
#}