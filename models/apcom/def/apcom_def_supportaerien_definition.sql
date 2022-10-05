{#
Definition / interface
- with the proper column types (thanks to _example_stg),
- but without any data (to allow to use to define columns in sql ex. as first in union)

Materialized as view because of these uses.

    alias='wrong'
#}

{{
  config(
    materialized="view"
  )
}}

{{ fdr_francedatareseau.definition(ref('apcom_def_supportaerien_example_stg')) }}