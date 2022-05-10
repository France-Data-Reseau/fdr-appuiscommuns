{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type canalisation de la source "osmgeodatamine_powersupports"
Partie générique - computed, utilisé dans osm_powsupp__apcom_supportaerien_translated

parameters :
- mapping_model_suffix : if set to ex. "osm" allows to use ex. l_pointaccueil_nature__osm rather than l_pointaccueil_nature
to compute Nature from Materiau
#}

{% macro apcom_supportaerien_translation__computed(translated_source_model_name, mapping_model_suffix="") %}

{% set sourceFieldPrefix = 'osmposup_' %}
{% set fieldPrefix = 'apcomsup_' %}

    -- simple join-less enrichment that does not hamper performance vs using the materialized table directly
    select
        {#{ dbt_utils.star(ref(translated_source_model_name), except=[
          fieldPrefix + "TypePhysique",
          fieldPrefix + "Nature"]) }#}
        {{ translated_source_model_name }}.*,
        --ST_Transform(geometry, 2154) as geometry_2154, -- Lambert 93 RATHER in union
        'APPUI' as "{{ fieldPrefix }}TypePhysique", -- toujours dans le cas d'usage (OSM : toujours pole ou tower)
        nature."Valeur" as "{{ fieldPrefix }}Nature" -- 'POTEAU BOIS'
        
    from {{ translated_source_model_name }}
        left join {{ ref('l_pointaccueil_nature' ~ mapping_model_suffix) }} nature -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on {{ translated_source_model_name }}."{{ fieldPrefix }}Materiau" = nature."{{ fieldPrefix }}Materiau"
          
{% endmacro %}