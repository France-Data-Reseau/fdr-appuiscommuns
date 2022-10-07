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

      {'columns': ['geometry_2154'], 'type': 'gist'},
#}

{% set fieldPrefix = 'apcomsup' + '_' %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

-- TODO on _src_id car IdSupportAerien pas unique globalement !(?)
{{
  config(
    materialized="incremental",
    unique_key=fieldPrefix + 'id',
    tags=["incremental"],
    indexes=[{'columns': ['"' + fieldPrefix + 'id"']},
      {'columns': order_by_fields},
      {'columns': ['geometry'], 'type': 'gist'},]
  )
}}


with unioned as (

{{ dbt_utils.union_relations(relations=[
      ref('apcom_def_supportaerien_definition'),
      ref('apcom_osm_supportaerien_deduped'),
      ref('apcom_birdz_supportaerien'),
      ref('apcom_aat_gthdv2_supportaerien'),
      ref('apcom_src_apcom_supportaerien')],
    include=(adapter.get_columns_in_relation(ref('apcom_def_supportaerien_definition')) | map(attribute='name') | list)
        + fdr_francedatareseau.list_generic_fields(fieldPrefix) + fdr_francedatareseau.list_import_fields(),
    source_column_name='apcomsup_src_relation',
    column_override={"geometry": "geometry", "geometry_2154": "geometry"})
}}

)

select *
-- adding stored geo field used to compute distances in dedupe :
, ST_Transform(geometry, 2154) as geometry_2154
, ST_X(geometry) as x, ST_Y(geometry) as y
from unioned
-- same order by as for _deduped : not really too long but index on it is enough
----order by "{{ order_by_fields | join('" asc, "') }}" asc -- NOO too long, index on it is enough

{% if is_incremental() %}
  where last_changed > (select max(last_changed) from {{ this }})
{% endif %}