{#
2 phase dedup - phase 1

or osm_powsupp__apcom_supportaerien_translated

54s on 1m osmposup without indexes on supportaerien, 50s with (so no change)

TODO from __deduped ?!
#}

{% set fieldPrefix = 'apcomsup' + '_' %}

{{
  config(
    materialized="table"
  )
}}

{{ apcom_supportaerien__2phase1link_fdrcommune_geometry(ref('appuiscommuns_supportaerien'), id_field=fieldPrefix + "Id") }}
