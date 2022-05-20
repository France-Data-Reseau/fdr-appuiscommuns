{{
  config(
    materialized="view"
  )
}}

{{ definition(ref('apcom_def_suivioccupation_example_stg')) }}