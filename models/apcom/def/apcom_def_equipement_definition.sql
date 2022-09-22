{{
  config(
    materialized="view"
  )
}}

{{ fdr_francedatareseau.definition(ref('apcom_def_equipement_example_stg')) }}