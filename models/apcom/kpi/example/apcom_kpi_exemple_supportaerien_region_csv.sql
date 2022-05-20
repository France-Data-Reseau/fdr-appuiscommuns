{#
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