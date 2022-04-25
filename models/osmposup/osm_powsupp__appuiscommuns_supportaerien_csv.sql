{#
version csv-isable (voir macro)

TODO generate
'osm_powsupp__appuiscommuns_supportaerien'
#}

{{
  config(
    materialized="view"
  )
}}

{{ to_csv() }}