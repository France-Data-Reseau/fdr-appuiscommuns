{# 
Example de profilage incrémental au fil du temps des données d'une source par le profiler de DBT Hub.
When enabled :
- only the last run of each period (ex. **week** for _otw) will be kept, to avoid accumulating too much data, because of unique_key="profiled_week"
(without, all runs would be kept, so ideally it should only be executed in a scheduled manner ex. once a week).
- TODO additionally, another _new model could compute only on new lines in this period, AT THE CONDITION normalization itself would be incremental
AND add updated_at, using if is_incremental() where updated_at > (select max(updated_at) from {{ this }}),
- but in both cases, since there is no updated_at provided BY the source, it can't be rebuilt and state must be kept, which requires
data migration if code (resp. profiling indicators, normalization) changes
#}

{{
  config(
    enabled=var("enableProfiling", false) | as_bool,
    materialized="incremental",
    unique_key="profiled_week"
  )
}}

select
  concat(substr(current_timestamp::text, 1, 4), EXTRACT('week' FROM current_timestamp)) as "profiled_week", -- '202221' ; using current_timestamp because profiled_at is varchar
  *
from {{ ref("profiledbt_source_appuiscommuns") }}