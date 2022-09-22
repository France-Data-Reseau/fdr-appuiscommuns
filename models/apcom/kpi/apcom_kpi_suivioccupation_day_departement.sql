{#
Indicateurs métier d'occupation, par région

Q "view" as in superset ?
Q rather in Superset ?!

TODO :
voir README

params :
- TODO start_date ? end_date ??

du JDB :

déclinés à différentes échelles géographiques : départementales, périmètre territorial de compétence du cas d'usage, communes.

Décompte total, segmenté par matériau
Décompte total, segmenté par exploitant électrique = AODE ?
Décompte total, segmenté par occupant télécoms = ??, technologie et cheminement = ?

Suivi de la dépose du cuivre
Représentation en histogramme ou courbe de l'évolution du nombre total de supports occupés par un réseau téléphonique cuivre (SupportAerienOccupation.Technologie='CUIVRE'), d'après les dates d'occupation connues dans le modèle d'échange.
Cette représentation pourra être déclinée à différentes échelles géographique comme précédemment.

Suivi de la montée en charge des déploiements
Représentation en histogramme ou courbe de l'évolution du nombre total de supports occupés par un réseau fibre (SupportAerienOccupation.Technologie='FIBRE'), d'après les dates d'occupation connues dans le modèle d'échange.
Cette représentation pourra être déclinée à différentes échelles géographique comme précédemment.
#}

{% set fieldPrefixSup = 'apcomsup_' %}
{% set fieldPrefixInd = 'apcomkpiocday_' %}

{{
  config(
    materialized="view",
  )
}}

{% set source_model = ref('apcom_kpi_suivioccupation_day') %}

with indicators as (
select
    count(*) as "{{ fieldPrefixInd }}all_count",
    COUNT(*) filter (where "apcomoc_Technologie" = 'CUIVRE') as "{{ fieldPrefixInd }}cuivre_count",
    COUNT(*) filter (where "apcomoc_Technologie" = 'FIBRE') as "{{ fieldPrefixInd }}fibre_count",

    COUNT(*) filter (where ("apcomsuoc_DebutOccupation" + "apcomsuoc_DureeOccupation" - INTERVAL '1 years' < "day")) as "{{ fieldPrefixInd }}expiration_1_an_count",
    COUNT(*) filter (where ("apcomsuoc_DebutOccupation" + "apcomsuoc_DureeOccupation" - INTERVAL '3 years' < "day")) as "{{ fieldPrefixInd }}expiration_3_an_count",
    COUNT(*) filter (where ("apcomsuoc_DebutOccupation" + "apcomsuoc_DureeOccupation" - INTERVAL '5 years' < "day")) as "{{ fieldPrefixInd }}expiration_5_an_count",

    {{ dbt_utils.pivot('"' + fieldPrefixSup + 'Materiau"', dbt_utils.get_column_values(source_model,
        '"' + fieldPrefixSup + 'Materiau"'), prefix=fieldPrefixSup + 'Materiau__') }},
    {{ dbt_utils.pivot('"apcomoc_Technologie"', dbt_utils.get_column_values(source_model,
        '"apcomoc_Technologie"'), prefix='apcomoc_Technologie__') }},
    -- TODO pivotS
    day,
    -- TODO aode
    dep_code,
    MIN("dep_name") as dep_name
    
    from {{ source_model }} apcom_occupation_day_enriched
    group by day, dep_code
)
select
    indicators.*
    --,
    --region."Geo Point", -- as geo_point_geojson, -- geojson for easy display NOO missing POINT( before 47.1,1.3 in osm so not geojson ! ; rename region."Geo Point" else error in _ot : syntax error at or near "text"LINE 8:                add column Geo Point text,
    --region."Geo Shape" -- as geo_shape_geojson -- geojson for easy display !
    ----region.geo_point_4326,
    ----region.geo_shape_4326 -- not useful here, not in CKAN import but in its transformation
    from indicators
    -- enrich with region : TODO move that to -region-enriched view
    ----{# left join {{ ref('georef-france-region.csv') }} region #} -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
    ----left join {{ source('france-data-reseau','georef-france-region_old.csv') }} region --  LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
    ----    on "reg_code" = region."Code Officiel Région"
    order by day asc