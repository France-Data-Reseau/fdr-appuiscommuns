{#
DESACTIVE par défaut, gardé à titre d'exemple de array_linked

Example d'exploitation - calcul d'indicateurs agrégés classiques, par commune :
- min et max, de numeric
- ensemble des valeurs rencontrées (dans une commune donc), pour une valeur de dictionnaire
#}

{% set fieldPrefix = 'apcomsup_' %}
{% set fieldPrefixInd = 'apcomexsupind_' %}

{{
  config(
    enabled=var("enableArrayLinked", false) | as_bool,
    materialized="view",
  )
}}

{% set source_model = ref('apcom_std_supportaerien_exemple_commune_array_demo_enriched') %}

select
    count(*) as "{{ fieldPrefixInd }}count",

    {# semantized names version
    "{{ fieldPrefix }}fdrcom_insee_id" as fdrcommune_insee_id,
    MIN("fdrcommune_nom") as fdrcommune_nom,
    MIN("fdrregion_insee_id") as fdrregion_insee_id,
    MIN("fdrregion_nom") as fdrregion_nom,
    #}
    "com_code" as com_code,
    MIN("com_name") as com_name,
    MIN("reg_code") as reg_code,
    MIN("reg_name") as reg_name,

    MIN("{{ fieldPrefix }}HauteurAppui") as "{{ fieldPrefixInd }}HauteurAppui__min",
    MAX("{{ fieldPrefix }}HauteurAppui") as "{{ fieldPrefixInd }}HauteurAppui__max",
    AVG("{{ fieldPrefix }}HauteurAppui") as "{{ fieldPrefixInd }}HauteurAppui__avg",

    count(*) / MIN("Population") as "{{ fieldPrefixInd }}count_per_inhabitant",

    array_agg(distinct "{{ fieldPrefix }}TypePhysique") as "{{ fieldPrefixInd }}TypePhysique__set",

    {{ dbt_utils.pivot('"' + fieldPrefix + 'TypePhysique"', dbt_utils.get_column_values(ref('apcom_std_supportaerien_unified'),
        '"' + fieldPrefix + 'TypePhysique"'), prefix=fieldPrefix + 'TypePhysique__') }},
    array_agg(distinct "{{ fieldPrefix }}Nature") as "{{ fieldPrefixInd }}Nature__set",
    {{ dbt_utils.pivot('"' + fieldPrefix + 'Nature"', dbt_utils.get_column_values(ref('apcom_std_supportaerien_unified'),
        '"' + fieldPrefix + 'Nature"'), prefix=fieldPrefix + 'Nature__') }},
    array_agg(distinct "{{ fieldPrefix }}Gestionnaire") as "{{ fieldPrefixInd }}Gestionnaire__set",
    {{ dbt_utils.pivot('"' + fieldPrefix + 'Gestionnaire"', dbt_utils.get_column_values(ref('apcom_std_supportaerien_unified'),
        '"' + fieldPrefix + 'Gestionnaire"'), prefix=fieldPrefix + 'Gestionnaire__') }},
    array_agg(distinct "{{ fieldPrefix }}Materiau") as "{{ fieldPrefixInd }}Materiau__set", -- TODO distinct
    {{ dbt_utils.pivot('"' + fieldPrefix + 'Materiau"', dbt_utils.get_column_values(ref('apcom_std_supportaerien_unified'),
        '"' + fieldPrefix + 'Materiau"'), prefix=fieldPrefix + 'Materiau__') }},

    -- geometry_shape_4326 would rather be gotten from a joined layer / relation :
    --min(geometry_shape_4326) as geometry_shape_4326,
    --min(geometry_center_4326) as geometry_center_4326

    -- for incremental / profiling :
    current_timestamp as updated_at
    
    from {{ source_model }}
    group by com_code --"{{ fieldPrefix }}fdrcom_insee_id"