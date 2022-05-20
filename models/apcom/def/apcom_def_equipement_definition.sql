{{
  config(
    materialized="view"
  )
}}

{{ definition(ref('apcom_def_equipement_example_stg')) }}