{#
Unification des données normalisées de toutes les sources de type appuiscommuns.occupation, en incrémental (table)

#}

{% set fieldPrefix = 'apcomoc_' %}

{{
  config(
    materialized="incremental",
    unique_key=fieldPrefix + 'id',
    tags=["incremental"],
  )
}}


with unioned as (

{{ dbt_utils.union_relations(relations=[
      ref('apcom_def_occupation_definition'),
      ref('apcom_src_apcom_occupation')],
    source_column_name='apcomoc_src_relation',)
}}

)

select * from unioned

{% if is_incremental() %}
  where last_changed > (select coalesce(max(last_changed), '1970-01-01T00:00:00') from {{ this }})
{% endif %}