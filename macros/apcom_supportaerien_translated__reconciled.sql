{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique
#}

{% macro apcom_supportaerien_translated__reconciled(translated_source_model_name) %}

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

with link_geometry_fdrcommune as (
    {%- set fields = adapter.get_columns_in_relation(ref(translated_source_model_name)) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
    {#% set cols = dbt_utils.star(ref(translated_source_model_name), except=[
          fieldPrefix + "fdrcommune__insee_id",
          fieldPrefix + "commune__insee_id",
          "fdrcommune__insee_id"]).split(', ') %#}
    {{ apcom_supportaerien_translation__link_geometry_fdrcommune(ref(translated_source_model_name), id_field=fieldPrefix+"Id", fields=fields) }}


{# NO RATHER dropping given commune ids and getting them from geometry
), bad_links_removed as (
    select
        {{ dbt_utils.star(ref(translated_source_model_name), except=[
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

& TODO LATER 2 phase dedup : phase 2 that joins on (approved / decided) link_geometry_fdrcommune :
), reconciled as (
    -- TODO index on geometryS (and commune as geometry) else orders of magnitude longer
    SELECT
        #}{# TODO rm except and therefore star ? requires _translation not to provide "best efforts" values of these fields #}{#
        {{ dbt_utils.star(ref(translated_source_model_name), relation_alias="link_geometry_fdrcommune", except=[
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
    FROM link_geometry_fdrcommune join link_candidate on link_geometry_fdrcommune."{{ fieldPrefix }}Id" = link_candidate."{{ fieldPrefix }}Id"

#}

), reconciled as (
    -- TODO index on geometryS (and commune as geometry) else orders of magnitude longer
    SELECT
        {{ dbt_utils.star(ref(translated_source_model_name), relation_alias="translation", except=[
          fieldPrefix + "fdrcommune__insee_id",
          fieldPrefix + "commune__insee_id",
          "fdrcommune__insee_id"]) }},
        --"{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        --"{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        link_geometry_fdrcommune."fdrcommune__insee_id" as "{{ fieldPrefix }}fdrcommune__insee_id",
        link_geometry_fdrcommune."fdrcommune__insee_id" as "{{ fieldPrefix }}commune__insee_id",
        link_geometry_fdrcommune."fdrcommune__insee_id" as "fdrcommune__insee_id",
        link_geometry_fdrcommune."fdrcommune__insee_id__arr",
        link_geometry_fdrcommune."fdrcommune__insee_id__arr_len"
    FROM {{ ref(translated_source_model_name) }} translation join link_geometry_fdrcommune on translation."{{ fieldPrefix }}Id" = link_geometry_fdrcommune."{{ fieldPrefix }}Id"

)

select * from reconciled
          
{% endmacro %}