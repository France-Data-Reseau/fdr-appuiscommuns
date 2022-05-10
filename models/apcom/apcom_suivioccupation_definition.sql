{{
  config(
    materialized="view"
  )
}}

{{ definition(ref('apcom_suivioccupation_example_stg')) }}