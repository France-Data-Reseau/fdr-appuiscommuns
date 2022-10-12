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

{% set fieldPrefix = 'apcomeq_' %}

{{
  config(
    materialized="incremental",
    unique_key=fieldPrefix + 'id',
    tags=["incremental"],
  )
}}


with unioned as (

{{ dbt_utils.union_relations(relations=[
      ref('apcom_def_equipement_definition'),
      ref('apcom_birdz_equipement'),
      ref('apcom_src_apcom_equipement')],
    include=(adapter.get_columns_in_relation(ref('apcom_def_equipement_definition')) | map(attribute='name') | list)
        + fdr_francedatareseau.list_generic_fields(fieldPrefix) + fdr_francedatareseau.list_import_fields(),
    source_column_name='apcomeq_src_relation',)
}}

)

select * from unioned

{% if is_incremental() %}
  where last_changed > (select coalesce(max(last_changed), '1970-01-01T00:00:00') from {{ this }})
{% endif %}