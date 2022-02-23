{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique
#}

{% macro appuiscommuns_supportaerien_source_common(normalized_source_model_name) %}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien_osmgeodatamine_powersupports_extract' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'osmpowersupports' %} -- ?
{% set prefix = 'appuiscommunssupp' %} -- ?+
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '__' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

with computed as (

    -- simple join-less enrichment that does not hamper performance vs using the materialized table directly
    select
        {{ dbt_utils.star(ref(normalized_source_model_name), except=[
          fieldPrefix + "TypePhysique",
          fieldPrefix + "Nature"]) }},
        'APPUI' as "{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        {{ ref('l_pointaccueil_nature__mapping') }}."{{ fieldPrefix }}Nature" -- 'POTEAU BOIS'
        
    from {{ ref(normalized_source_model_name) }}
        left join {{ ref('l_pointaccueil_nature__mapping') }} -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on {{ ref(normalized_source_model_name) }}."{{ fieldPrefix }}Materiau" = {{ ref('l_pointaccueil_nature__mapping') }}."Valeur"

{# NO RATHER dropping given commune ids and getting them from geometry
), bad_links_removed as (
    select
        {{ dbt_utils.star(ref(normalized_source_model_name), except=[
          fieldPrefix + "fdrcommune__insee_id",
          fieldPrefix + "commune__insee_id",
          "fdrcommune__insee_id"]) }},
        "{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        "{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        c.com_code as "{{ fieldPrefix }}fdrcommune__insee_id",
        c.com_code as "{{ fieldPrefix }}commune__insee_id",
        c.com_code as "fdrcommune__insee_id"
        -- TODO & _nom ?
        
    from computed
        left join {{ source('france-data-reseau', 'georef-france-commune.csv') }} c -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on computed."appuiscommunssupp__fdrcommune__insee_id" = c.com_code

#}
), link_candidates as (
    -- 5s on 1m lines
    select
        computed."{{ fieldPrefix }}Id",
        c.com_code as "fdrcommune__insee_id"
    --FROM computed, {{ source('france-data-reseau', 'georef-france-commune.csv') }} c
    FROM computed, {{ ref('georef-france-commune.csv') }} c
    WHERE ST_Contains(ST_GeometryFromText(ST_AsText(c.geo_shape), 4326), computed.geometry) and c.com_code is not null -- TODO patch source geometry to 4326 SRID

), link_candidate_array as (
    -- no performance change, else 2 array_agg would gave to be inlined ;
    -- TODO and "updated" or row_count() for ORDER BY LIMIT 1 ? LATER macro & FILTER NOT NULL http///
    select
        "{{ fieldPrefix }}Id",
        --(ARRAY_AGG("fdrcommune__insee_id") FILTER (WHERE "fdrcommune__insee_id" IS NOT NULL))[1] as "fdrcommune__insee_id", -- see https://stackoverflow.com/questions/61874745/postgresql-get-first-non-null-value-per-group
        ARRAY_AGG("fdrcommune__insee_id") as "fdrcommune__insee_id__arr",
        count(*) as "fdrcommune__insee_id__arr_len"
    from link_candidates
    group by "{{ fieldPrefix }}Id"
), link_candidate as (
    -- no performance change
    select
        "{{ fieldPrefix }}Id",
        ("fdrcommune__insee_id__arr")[1] as "fdrcommune__insee_id",
        "fdrcommune__insee_id__arr",
        "fdrcommune__insee_id__arr_len"
    from link_candidate_array

    {#
), reconciled as (
    -- TODO index on geometryS (and commune as geometry) else orders of magnitude longer
    SELECT
        {{ dbt_utils.star(ref(normalized_source_model_name), relation_alias="computed", except=[
          fieldPrefix + "fdrcommune__insee_id",
          fieldPrefix + "commune__insee_id",
          "fdrcommune__insee_id"]) }},
        --"{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        --"{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        link_candidate."fdrcommune__insee_id" as "{{ fieldPrefix }}fdrcommune__insee_id",
        link_candidate."fdrcommune__insee_id" as "{{ fieldPrefix }}commune__insee_id",
        link_candidate."fdrcommune__insee_id" as "fdrcommune__insee_id",
        link_candidate."fdrcommune__insee_id__arr",
        link_candidate."fdrcommune__insee_id__arr_len"
    FROM computed join link_candidate on computed."{{ fieldPrefix }}Id" = link_candidate."{{ fieldPrefix }}Id"
#}
)

select * from link_candidate
          
{% endmacro %}