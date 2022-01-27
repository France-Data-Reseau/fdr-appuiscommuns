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

{#
Alternative : implicit SELECT * or=dbt_utils.star(my_model_definition_relation) or all fields explicitly...
#}
select
    {{ dbt_utils.star(ref('appuiscommuns_supportaerien__definition')) }},
    {{ dbt_utils.star(source('france-data-reseau', 'georef-france-commune.csv')) }}
    from {{ ref('appuiscommuns_supportaerien') }}
    join {{ source('france-data-reseau', 'georef-france-commune.csv') }}
        on {{ ref('appuiscommuns_supportaerien') }}."appuiscommunssupp__fdrcommune__insee_id"::text = {{ source('france-data-reseau', 'georef-france-commune.csv') }}.com_code