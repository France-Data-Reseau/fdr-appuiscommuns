{#
Enrichissement (par les communes) des données normalisées de toutes les sources de type appuiscommuns.supportaerien.

- on ne garde que les champs officiels
#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set typeName = 'Canalisation' %}
{% set prefix = 'appuiscommunssupp' %} -- ?
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{{
  config(
    materialized="view"
  )
}}

with enriched as (
{#
Alternative : implicit SELECT * or=dbt_utils.star(my_model_definition_relation) or all fields explicitly...
#}
select
    {{ dbt_utils.star(ref('appuiscommuns_supportaerien__definition')) }},
    {{ dbt_utils.star(source('france-data-reseau', 'georef-france-commune.csv'), except=['_id', '_full_text']) }}, -- _id is most probably added by CKAN to all imports
    {{ dbt_utils.star(source('france-data-reseau', 'INSEE communes données démographiques'), except=['_id', '_full_text']) }} -- _id is most probably added by CKAN to all imports
    from {{ ref('appuiscommuns_supportaerien') }}
    CROSS JOIN unnest({{ ref('appuiscommuns_supportaerien') }}."fdrcommune__insee_id__arr") supp("fdrcommune__insee_id__arr_u")
    left join {{ source('france-data-reseau', 'georef-france-commune.csv') }} -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on supp."fdrcommune__insee_id__arr_u" = {{ source('france-data-reseau', 'georef-france-commune.csv') }}.com_code
    left join {{ source('france-data-reseau', 'INSEE communes données démographiques') }} -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on {{ ref('appuiscommuns_supportaerien') }}."fdrcommune__insee_id" = {{ source('france-data-reseau', 'INSEE communes données démographiques') }}."CODGEO"
)
select * from enriched