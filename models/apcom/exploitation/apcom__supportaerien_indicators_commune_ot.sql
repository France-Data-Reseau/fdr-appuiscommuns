{#

Example de profilage incrémental au fil du temps des données d'une source par le profiler de DBT Hub.
Si activé (typiquement de manière planifiée : chaque semaine...) :
- pas de if is_incremental() where profiled_at > (select max(updated_at) from {{ this }}),
donc toutes les lignes (autant d'indicateurs) sont recalculée A CHAQUE FOIS
donc idéalement ne l'exécuter que de manière planifiée (chaque semaine...)
- ou alors mettre une unique_key (période ex. semaine / mois du current_timestamp), sinon en append only i.e. aucune n'est mise à jour.

on_schema_change='append_new_columns' : enable incremental schema update ;
- TODO for now KO because doesn't quote field https://github.com/dbt-labs/dbt-core/issues/4423
- requires project conf ; else ex. column "appuiscommunssupp__Gestionnaire__" does not exist LINE 13: ..._set", "appuiscommunssupp__Gestionnaire__Enedis", "appuiscom...

TODO generate ??
'apcom__supportaerien_indicators_commune'
#}

{% set source_model = ref(model.name[:-3]) %}

{% set prefix = 'apcomsup' %} -- ?
{% set fieldPrefix = prefix + '_' %}

{{
  config(
    enabled=var("enableOverTime", false) | as_bool,
    materialized="incremental",
    unique_key='concat(profiled_week, "' + fieldPrefix + 'com_code")'
  )
}}

select
  concat(substr("updated_at"::text, 1, 4), EXTRACT('week' FROM "updated_at")) as "profiled_week", -- '202221'
  {{ dbt_utils.star(source_model) }}
from {{ source_model }}