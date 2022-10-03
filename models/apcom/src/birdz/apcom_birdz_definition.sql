{#
NOT USED in _parsed
#}

{{
  config(
    materialized="view"
  )
}}

{{ fdr_francedatareseau.definition(ref(this.name | replace('_definition', '_example_stg'))) }}