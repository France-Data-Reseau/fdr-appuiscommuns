{#
Unification des données normalisées de toutes les sources de type appuiscommuns.supportaerien

60s

Union using dbt_utils helper :
- _definition (with 0 data lines) as the first unioned relation adds even fields missing in all normalizations, with the right type,
if they are provided in the official type definition
- include=dbt_utils.star(_definition) excludes source-specific fields
- column_override={"geometry": "geometry"} is required else syntax error : cast("geometry" as USER-DEFINED) as "geo...
see https://github.com/dbt-labs/dbt-utils#union_relations-source
- source_column_name : apcomsup_src_relation and not default _dbt_source_relation to avoid conflicts in joins
(TODO Q vs apcomsup_src_name would ?)
- 

is a table only if has reconciliation or dedup between sources
    TODO include=dbt_utils.star(ref('acom_def_supportaerien_definition')),
#}

{% set fieldPrefix = 'apcomsup' + '_' %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'Id"']},
      {'columns': order_by_fields},
      {'columns': ['geometry'], 'type': 'gist'},
      {'columns': ['geometry_2154'], 'type': 'gist'},]
  )
}}


with unioned as (

{{ dbt_utils.union_relations(relations=[
      ref('apcom_def_supportaerien_definition'),
      ref('apcom_osm_supportaerien_deduped'),
      ref('apcom_birdz_supportaerien'),
      ref('apcom_aat_gthdv2_supportaerien'),
      source_or_test_ref('appuiscommuns', 'apcom_def_supportaerien')],
    source_column_name='apcomsup_src_relation',
    column_override={"geometry": "geometry", "geometry_2154": "geometry"})
}}

{#
14s without commune_linked
), geometry_deduped as (
    -- geometry deduplication :
    -- NOT HERE, REQUIRES MOSTLY NEAR RATHER THAN EXACT GEO DEDUPLICATION AMONG DIFFERENT SOURCCES
    -- FOR MORE PERFORMANCE, REQUIRES PRIMARY KEY ON ID AND A TABLE SO NOT ON SOURCE
    -- OK : 44s rather than 0,44 if on 1m lines rather than the 200 lines, even on translation view (or source view)
    {{ dedupe('unioned', id_fields=['"geometry"']) }}
    
#}
{#
commune linking :
done outside, because n-n relationship linking produces another table
and 1-n linking is kept for example and reuses this model
NB. way too long without table materialization and indexing
), commune_linked as (
    -- reconciliation :
    -- NB. reconciliation to communes requires a geometry field, so can't be done on the source (and is more efficient being in a table)
    -- moreover, commune is not necessary for other translation handlings (dedup...). And doing it after translation allows to do it all in one go.
    -- TODO remove
    {%- set fields = ['_dbt_source_relation', 'appuiscommunssupp__src_priority'] + adapter.get_columns_in_relation(ref('apcom_def_supportaerien_definition')) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
    -- (no need to except=[apcomsup_com_code"] because in the ex. osm source it is osmposup_com_code)
    {#% set cols = dbt_utils.star(sourceModel).split(',') %# OLD}
    {{ apcom_supportaerien_translation__link_geometry_fdrcommune("unioned", id_field="apcomsup_Id", fields=fields) }}

#}
)

select *,
ST_Transform(geometry, 2154) as geometry_2154
from unioned
-- TOO LONG same order by as for _deduped :
--order by apcomsup_src_priority asc, apcomsup_src_id asc
order by "{{ order_by_fields | join('" asc, "') }}" asc


{#
Alternative : explicit SELECT * or all fields explicitly UNION...
with source as (

    select
        "{{ fieldPrefix }}src_name",
        --"{{ fieldPrefix }}src_index",
        "{{ fieldPrefix }}src_id",
        "{{ fieldPrefix }}Id",
        geometry, -- OU prefix ? forme ??
        --"{{ sourceFieldPrefix }}utility", -- power
        --"{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        "{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        "{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        "{{ fieldPrefix }}Gestionnaire",
        "{{ fieldPrefix }}Materiau", -- TODO dict conv
        "{{ fieldPrefix }}HauteurAppui", -- TODO Hauteur ! hauteur ? __m ??
        "{{ fieldPrefix }}CodeExterne", -- 101, 87, 37081ER073...
        --"{{ sourceFieldPrefix }}line_attachment", -- suspension, pin, anchor... MAIS QUE CompositionAppui (plein), StructureAppui (moise)
        --"{{ sourceFieldPrefix }}line_management", -- split, branch, cross... MAIS QUE CompositionAppui (plein), StructureAppui (moise)
        --"{{ sourceFieldPrefix }}transition", -- yes
        "{{ fieldPrefix }}fdrcommune__insee_id",
        "{{ fieldPrefix }}fdrcommune__nom"
        
        from {{ ref('sample__appuiscommuns_extract_supportaerien') }}
    
    --UNION...

)

select * from source
#}