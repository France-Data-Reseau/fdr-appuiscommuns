{#
Unification des données normalisées de toutes les sources de type appuiscommuns.suivioccupation, en incrémental (table)

#}

{% set fieldPrefix = 'apcomsuoc_' %}

{{
  config(
    materialized="incremental",
    unique_key=fieldPrefix + 'id',
    tags=["incremental"],
  )
}}


with unioned as (

{{ dbt_utils.union_relations(relations=[
      ref('apcom_def_suivioccupation_definition'),
      ref('apcom_src_apcom_suivioccupation')],
    source_column_name='apcomsuoc_src_relation',)
}}

)

select * from unioned

{% if is_incremental() %}
  where last_changed > (select coalesce(max(last_changed), to_timestamp('1970-01-01T00:00:00', 'YYYY-MM-DDTHH24:MI:SS')) from {{ this }})
{% endif %}