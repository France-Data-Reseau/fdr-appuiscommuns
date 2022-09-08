{#
OBSOLETE plutôt utiliser sripts/publish.py (qui CSV-ise à l'aide de pandas de manière générique)

version csv-isable (voir macro)

TODO generate
'apcom__supportaerien_indicators_region'
#}

{{
  config(
    materialized="view"
  )
}}

{{ to_csv(ref(model.name[:-4])) }}