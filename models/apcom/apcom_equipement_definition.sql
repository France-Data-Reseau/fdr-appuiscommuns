{{
  config(
    materialized="view"
  )
}}

{{ definition(ref('apcom_equipement_example_stg')) }}