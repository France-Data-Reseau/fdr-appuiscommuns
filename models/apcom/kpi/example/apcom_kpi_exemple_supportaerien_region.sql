{#
Example d'exploitation - calcul d'indicateurs agrégés classiques, par commune :
- min et max, de numeric
- ensemble des valeurs rencontrées (dans une commune donc), pour une valeur de dictionnaire
#}

{% set source_model = ref('apcom_std_supportaerien_com_demo_enriched') %}

{% set fieldPrefix = 'apcomsup_' %}
{% set fieldPrefixInd = 'apcomexsupind_' %}

with region_stat as (
select
    count(*) as "{{ fieldPrefixInd }}count",
    {# semantized names version
    fdrregion_insee_id,
    #}
    MIN("reg_name") as reg_name,
    reg_code,
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
    current_timestamp as updated_at
    
    from {{ source_model }}
    group by reg_code --"fdrregion_insee_id"
)
select
    region_stat.*,
    --region."Geo Point", -- as geo_point_geojson, -- geojson for easy display NOO missing POINT( before 47.1,1.3 in osm so not geojson ! ; rename region."Geo Point" else error in _ot : syntax error at or near "text"LINE 8:                add column Geo Point text,
    --region."Geo Shape" -- as geo_shape_geojson -- geojson for easy display !
    region.geo_point_4326,
    region.geo_shape_4326 -- not useful here, not in CKAN import but in its transformation
    from region_stat
    -- enrich with region : TODO move that to -region-enriched view
    {# left join {{ ref('georef-france-region.csv') }} region #} -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
    left join {{ source('france-data-reseau', 'georef-france-region_old.csv') }} region -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on "reg_code" = region."Code Officiel Région"