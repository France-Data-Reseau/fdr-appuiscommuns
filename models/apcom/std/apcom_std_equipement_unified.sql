{#
Unification des données normalisées de toutes les sources de type appuiscommuns.equipement, en incrémental (table)

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
  where last_changed > (select coalesce(max(last_changed), to_timestamp('1970-01-01T00:00:00', 'YYYY-MM-DD"T"HH24:MI:SS')) from {{ this }})
{% endif %}