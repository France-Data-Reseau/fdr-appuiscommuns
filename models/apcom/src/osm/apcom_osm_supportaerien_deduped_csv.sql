{#
OBSOLETE plutôt utiliser sripts/publish.py (qui CSV-ise à l'aide de pandas de manière générique)

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