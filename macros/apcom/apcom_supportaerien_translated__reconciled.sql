{#
OBSOLETE utilisé dans une partie commentée de osm supportaerien _deduped, utilise array_link (déconseillé)

Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique
#}

{% macro apcom_supportaerien_translated__reconciled(translated_source_model_name) %}

{% set sourceFieldPrefix = 'osmposup_' %}
{% set fieldPrefix = 'apcomsup_' %}

with link_geometry_fdrcommune as (
    {%- set fields = adapter.get_columns_in_relation(ref(translated_source_model_name)) | map(attribute="name") | list -%}-- BEWARE without | list it stays a generator that can only be iterated once
    {#% set cols = dbt_utils.star(ref(translated_source_model_name), except=[
          sourceFieldPrefix + "com_code"]).split(', ') %#}
    {{ apcom_supportaerien_array_link_geometry_commune(ref(translated_source_model_name), id_field=fieldPrefix+"Id", fields=fields) }}


{# NO RATHER dropping given commune ids and getting them from geometry
), bad_links_removed as (
    select
        -- (no need to except=[apcomsup_com_code"] because in the ex. osm source it is osmposup_com_code)
        {{ dbt_utils.star(ref(translated_source_model_name)) }},
        "{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        "{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        c.com_code as "{{ fieldPrefix }}com_code"
        -- TODO & _nom ?
        
    from computed
        left join {{ source('france-data-reseau', 'fdr_src_communes_ods') }} c -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on computed."{{ sourceFieldPrefix }}com_code" = c.com_code

& TODO LATER 2 phase dedup : phase 2 that joins on (approved / decided) link_geometry_fdrcommune :
), reconciled as (
    -- TODO index on geometryS (and commune as geometry) else orders of magnitude longer
    SELECT
        -- TODO rm except and therefore star ? requires _translation not to provide "best efforts" values of these fields
        -- (no need to except=[apcomsup_com_code"] because in the ex. osm source it is osmposup_com_code)
        {{ dbt_utils.star(ref(translated_source_model_name), relation_alias="link_geometry_fdrcommune") }},
        --"{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        --"{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        link_candidate."{{ fieldPrefix }}com_code" as "{{ fieldPrefix }}com_code",
        link_candidate."{{ fieldPrefix }}com_code__arr",
        link_candidate."{{ fieldPrefix }}com_code__arr_len"
    FROM link_geometry_fdrcommune
    join link_candidate on link_geometry_fdrcommune."{{ fieldPrefix }}Id" = link_candidate."{{ fieldPrefix }}Id"

#}

), reconciled as (
    -- TODO index on geometryS (and commune as geometry) else orders of magnitude longer
    SELECT
        {{ dbt_utils.star(ref(translated_source_model_name), relation_alias="translation", except=[
          sourceFieldPrefix + "com_code"]) }},
        --"{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        --"{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        link_geometry_fdrcommune."{{ fieldPrefix }}com_code" as "{{ fieldPrefix }}com_code",
        link_geometry_fdrcommune."{{ fieldPrefix }}com_code__arr",
        link_geometry_fdrcommune."{{ fieldPrefix }}com_code__arr_len"
    FROM {{ ref(translated_source_model_name) }} translation
    join link_geometry_fdrcommune on translation."{{ fieldPrefix }}Id" = link_geometry_fdrcommune."{{ fieldPrefix }}Id"

)

select * from reconciled
          
{% endmacro %}