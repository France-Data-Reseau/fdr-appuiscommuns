{#
Unification des données normalisées de toutes les sources de type appuiscommuns.supportaerien

Union using dbt_utils helper :
- _definition (with 0 data lines) as the first unioned relation adds even fields missing in all normalizations, with the right type,
if they are provided in the official type definition
- include=dbt_utils.star(_definition) excludes source-specific fields
- source_column_name="_dbt_source_relation"
-

is a table only if has reconciliation or dedup between sources
    include=dbt_utils.star(ref('apcom_supportaerien_definition')),
#}

{{
  config(
    materialized="table"
  )
}}


with unioned as (

{{ dbt_utils.union_relations(relations=[
      ref('apcom_def_suivioccupation_definition'),
      ref('apcom_src_apcom_suivioccupation_parsed')],
    source_column_name='apcomsuoc_src_relation',)
}}

)

select * from unioned