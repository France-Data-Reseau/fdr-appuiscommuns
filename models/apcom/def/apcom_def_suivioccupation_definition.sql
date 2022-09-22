{{
  config(
    materialized="view"
  )
}}

{{ fdr_francedatareseau.definition(ref('apcom_def_suivioccupation_example_stg')) }}