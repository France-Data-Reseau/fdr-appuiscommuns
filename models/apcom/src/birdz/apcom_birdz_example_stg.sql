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
        except=[]) }} -- NOT retyping ADR_POS_X,C,254 ADR_POS_Y,C,254 because french floating point format, nor "POSE_DATE,C,254" because french date format
from {{ source_model }}