{#
OBSOLETE plutôt utiliser sripts/publish.py (qui CSV-ise à l'aide de pandas de manière générique)

version csv-isable (voir macro)

TODO generate
#}

{{
  config(
    materialized="view"
  )
}}

{{ fdr_francedatareseau.to_csv(none) }}