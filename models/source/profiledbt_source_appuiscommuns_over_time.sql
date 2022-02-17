{# 
Example de profilage incrémental au fil du temps des données d'une source par le profiler de DBT Hub :
#}

{#{
  config(
    materialized="incremental"
  )
}#}

select
  *
from {{ ref("profiledbt_source_appuiscommuns") }}