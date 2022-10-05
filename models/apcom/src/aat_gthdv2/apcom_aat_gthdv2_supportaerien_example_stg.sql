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
        except=["geometry"]) }} -- NOT retyping ADR_POS_X,C,254 ADR_POS_Y,C,254 because french floating point format, nor "POSE_DATE,C,254" because french date format
    ,
    ST_GeomFROMText("geometry", 2154) as "geometry",
    'apcom_aat_gthdv2' as "FDR_SOURCE_NOM",
    'example' as "data_owner_id",
    'example' as "import_table"
from {{ source_model }}