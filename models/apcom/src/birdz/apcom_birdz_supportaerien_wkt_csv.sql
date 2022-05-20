{#
version csv-isable (voir macro)

TODO generate
'birdz__apcom_supportaerien'
#}

{{
  config(
    materialized="view"
  )
}}

{{ to_csv(none, true) }}