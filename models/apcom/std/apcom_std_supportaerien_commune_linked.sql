{#
Produces the table (materialized because computes) of the n-n relationship between apcomsup and commune

120s on apcomsup _unified
OLD 54s on 1m osmposup without indexes on supportaerien, 50s with (so no change)

TODO from __deduped ?!
#}

{% set fieldPrefix = 'apcomsup' + '_' %}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'IdSupportAerien"']},
      {'columns': ['com_code']},
    ]
  )
}}

{% set source_model = ref('apcom_std_supportaerien_unified') %}

{{ apcom_supportaerien_2phase1link_commune_geometry(source_model, id_field=fieldPrefix + "IdSupportAerien") }}
