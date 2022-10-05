{#
NOT USED through _definition in _parsed
#}

{{
  config(
    materialized="view"
  )
}}

{% set source_model = ref(this.name | replace('_stg', '')) %}

select
    {{ dbt_utils.star(source_model,
        except=["X", "Y", "height"]) }}, -- retyping fields (not required)
        "X"::numeric,
        "Y"::numeric,
        {{ fdr_francedatareseau.to_numeric_or_null("height", source_model) }} as "height" -- flacombe : et non HauteurTotal ! TODO H/hauteur ? __m ?? car "emental" dans les données 1m lignes
    ,
    'apcom_osm' as "FDR_SOURCE_NOM",
    'example' as "data_owner_id",
    'example' as "import_table"

from {{ source_model }}