{#
NOT USED through _definition in _parsed

TODO remove ,C,254 etc. from column names as done in aat_gthtv2 ?
#}

{{
  config(
    materialized="view"
  )
}}

{% set source_model = ref(this.name | replace('_stg', '')) %}

select
    {{ dbt_utils.star(source_model,
        except=[]) }} -- NOT retyping ADR_POS_X,C,254 ADR_POS_Y,C,254 because french floating point format, nor "POSE_DATE,C,254" because french date format
    ,
    'apcom_equip_birdz' as "FDR_SOURCE_NOM",
    'example' as "data_owner_id",
    'example' as "import_table"
from {{ source_model }}