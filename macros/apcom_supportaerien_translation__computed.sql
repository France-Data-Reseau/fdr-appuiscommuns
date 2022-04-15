{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique - computed, utilisé dans osm_powsupp__apcom_supportaerien_translated
#}

{% macro apcom_supportaerien_translation__computed(translated_source_model_name) %}

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

    -- simple join-less enrichment that does not hamper performance vs using the materialized table directly
    select
        {#{ dbt_utils.star(ref(translated_source_model_name), except=[
          fieldPrefix + "TypePhysique",
          fieldPrefix + "Nature"]) }#}{{ translated_source_model_name }}.*,
        'APPUI' as "{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        {{ ref('l_pointaccueil_nature__mapping') }}."{{ fieldPrefix }}Nature" -- 'POTEAU BOIS'
        
    from {{ translated_source_model_name }}{#{ ref(translated_source_model_name) }#}
        left join {{ ref('l_pointaccueil_nature__mapping') }} -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on {{ translated_source_model_name }}{#{ ref(translated_source_model_name) }#}."{{ fieldPrefix }}Materiau" = {{ ref('l_pointaccueil_nature__mapping') }}."Valeur"
          
{% endmacro %}