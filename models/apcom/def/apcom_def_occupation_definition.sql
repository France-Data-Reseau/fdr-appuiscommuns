{#
NB. équipement traverse a plusieurs occupations ex. une par trou ; donc pas relation 1-1 et pas dénormalisable
#}

{{
  config(
    materialized="view"
  )
}}

{{ fdr_francedatareseau.definition(ref('apcom_def_occupation_example_stg')) }}