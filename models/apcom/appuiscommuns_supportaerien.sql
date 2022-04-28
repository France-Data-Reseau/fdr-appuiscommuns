{#
Unification des données normalisées de toutes les sources de type appuiscommuns.supportaerien
#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set typeName = 'SupportAerien' %}
{% set prefix = 'appuiscommunssupp' %} -- ?
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{#
Union using dbt_utils helper :
- _definition (with 0 data lines) as the first unioned relation adds even fields missing in all normalizations, with the right type,
if they are provided in the official type definition
- include=dbt_utils.star(_definition) excludes source-specific fields
- column_override={"geometry": "geometry"} is required else syntax error : cast("geometry" as USER-DEFINED) as "geo...
see https://github.com/dbt-labs/dbt-utils#union_relations-source
- source_column_name="_dbt_source_relation"
- 

is a table only if has reconciliation
    include=dbt_utils.star(ref('appuiscommuns_supportaerien__definition')),
#}

{{
  config(
    materialized="table"
  )
}}


with all1 as (

{{ dbt_utils.union_relations(relations=[
      ref('appuiscommuns_supportaerien__definition'),
      ref('megalis__apcom_supportaerien'),
      ref('birdz__apcom_supportaerien'),
      ref('osm_powsupp__appuiscommuns_supportaerien')],
    column_override={"geometry": "geometry"})
}}

{#
14s without commune_linked
), geometry_deduped as (
    -- geometry deduplication :
    -- deduplication could 
    -- FOR MORE PERFORMANCE, REQUIRES PRIMARY KEY ON ID AND A TABLE SO NOT ON SOURCE
    -- OK : 44s rather than 0,44 if on 1m lines rather than the 200 lines, even on translation view (or source view)
    {{ dedupe('all1', id_fields=['"geometry"']) }}
    
#}
{# TODO rather n-n relaationship
NB. way too long without table materialization and indexing
#}
), commune_linked as (
    -- reconciliation :
    -- NB. reconciliation to communes requires a geometry field, so can't be done on the source (and is more efficient being in a table)
    -- moreover, commune is not necessary for other translation handlings (dedup...). And doing it after translation allows to do it all in one go.
    {%- set fields = ['_dbt_source_relation'] + adapter.get_columns_in_relation(ref('appuiscommuns_supportaerien__definition')) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
    {#% set cols = dbt_utils.star(sourceModel, except=[
          fieldPrefix + "fdrcommune__insee_id",
          fieldPrefix + "commune__insee_id",
          "fdrcommune__insee_id"]).split(',') %#}
    {{ apcom_supportaerien_translation__link_geometry_fdrcommune("all1", id_field="appuiscommunssupp__Id", fields=fields) }}
)

select * from commune_linked


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