{#
Produces the table (materialized because computes) of the n-n relationship between apcomsup and commune,
materialized as DBT incremental (filtered on apcom_std_supportaerien_unified last_changed)

120s on apcomsup _unified
OLD 54s on 1m osmposup without indexes on supportaerien, 50s with (so no change)
#}

{% set fieldPrefix = 'apcomsup' + '_' %}

{{
  config(
    materialized="incremental",
    unique_key=['"' + fieldPrefix + "id" + '"', 'com_code'],
    tags=["incremental", "long"],
    indexes=[
      {'columns': ['"' + fieldPrefix + 'id"']},
      {'columns': ['com_code']},
    ]
  )
}}

{% set source_model = ref('apcom_std_supportaerien_unified') %}

{{ apcom_supportaerien_2phase1link_commune_geometry(source_model,
    id_field=fieldPrefix + "id", geometry_field="geometry") }}
