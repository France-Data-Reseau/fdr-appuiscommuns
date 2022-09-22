{#
Example d'exploitation - calcul d'indicateurs agrégés classiques, par commune :
- min et max, de numeric
- ensemble des valeurs rencontrées (dans une commune donc), pour une valeur de dictionnaire
#}

{% set fieldPrefix = 'apcomsup_' %}
{% set fieldPrefixInd = 'apcomexsupind_' %}

{% set source_model = ref('apcom_std_supportaerien_com_demo_enriched') %}

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
    array_agg(distinct "{{ fieldPrefix }}TypePhysique") as "{{ fieldPrefixInd }}TypePhysique__set",
    {{ dbt_utils.pivot('"' + fieldPrefix + 'TypePhysique"', dbt_utils.get_column_values(source_model,
        '"' + fieldPrefix + 'TypePhysique"'), prefix=fieldPrefix + 'TypePhysique__') }},
    array_agg(distinct "{{ fieldPrefix }}Nature") as "{{ fieldPrefixInd }}Nature__set",
    {{ dbt_utils.pivot('"' + fieldPrefix + 'Nature"', dbt_utils.get_column_values(source_model,
        '"' + fieldPrefix + 'Nature"'), prefix=fieldPrefix + 'Nature__') }},
    array_agg(distinct "{{ fieldPrefix }}Gestionnaire") as "{{ fieldPrefixInd }}Gestionnaire__set",
    {{ dbt_utils.pivot('"' + fieldPrefix + 'Gestionnaire"', dbt_utils.get_column_values(source_model,
        '"' + fieldPrefix + 'Gestionnaire"'), prefix=fieldPrefix + 'Gestionnaire__') }},
    array_agg(distinct "{{ fieldPrefix }}Materiau") as "{{ fieldPrefixInd }}Materiau__set", -- TODO distinct
    {{ dbt_utils.pivot('"' + fieldPrefix + 'Materiau"', dbt_utils.get_column_values(source_model,
        '"' + fieldPrefix + 'Materiau"'), prefix=fieldPrefix + 'Materiau__') }},
    MIN("{{ fieldPrefix }}HauteurAppui") as "{{ fieldPrefixInd }}HauteurAppui__min",
    MAX("{{ fieldPrefix }}HauteurAppui") as "{{ fieldPrefixInd }}HauteurAppui__max",
    AVG("{{ fieldPrefix }}HauteurAppui") as "{{ fieldPrefixInd }}HauteurAppui__avg",
    count(*) / MIN("Population") as "{{ fieldPrefixInd }}count_per_inhabitant",
    current_timestamp as updated_at,
    min(geo_shape_4326) as geo_shape_4326,
    min(geo_point_4326) as geo_point_4326
    
    from {{ source_model }}
    group by com_code --"{{ fieldPrefix }}fdrcom_insee_id"