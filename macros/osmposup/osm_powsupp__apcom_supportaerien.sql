{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type supportaerien
de la source "osmgeodatamine_powersupports"
Partie spécifique à la source

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la __definition) ?
#}

{% macro osm_powsupp__apcom_supportaerien(source_relation, src_priority=None) %}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien_osmgeodatamine_powersupports_extract' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'osmposup' %} -- ?
{% set prefix = 'apcomsup' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '_' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '_' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

with source as (

    select * from {{ source_relation }}
    {% if var('limit', 0) > 0 %}
    LIMIT {{ var('limit') }}
    {% endif %}

),

renamed as (

    select
        '{{ source_relation }}' as "{{ fieldPrefix }}src_name", -- source name (else won't have it anymore once unified with other sources)
        --id as "{{ fieldPrefix }}src_index", -- index in source
        "osm_id"::text as "{{ fieldPrefix }}src_id", -- source own id
        "X"::numeric, -- as "{{ sourceFieldPrefix }}x",
        "Y"::numeric, -- as "{{ sourceFieldPrefix }}x",
        "utility"::text as "{{ sourceFieldPrefix }}utility", -- power
        "nature"::text as "{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        "operator"::text as "{{ fieldPrefix }}Gestionnaire",
        "material"::text as "{{ sourceFieldPrefix }}material", -- TODO dict conv
        "height"::text as "{{ fieldPrefix }}HauteurAppui__s", -- flacombe : et non HauteurTotal ! TODO H/hauteur ? __m ?? car "emental" dans les données 1m lignes
        "reference"::text as "{{ fieldPrefix }}CodeExterne", -- ?? 101, 87, 37081ER073...
        "line_attachment"::text as "{{ sourceFieldPrefix }}line_attachment", -- ? suspension, pin, anchor...
        "line_management"::text as "{{ sourceFieldPrefix }}line_management", -- ? split, branch, cross...
        "transition"::text as "{{ sourceFieldPrefix }}transition", -- ? yes
        --"com_insee"::text as "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        --"com_insee"::text as "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        --"com_insee"::text as "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        --"com_nom"::text as "{{ fieldPrefix }}commune_nom", -- enrichissement mminimal pour rendre code insee lisible ?
        --"com_nom"::text as "{{ fieldPrefix }}fdrcommune__nom" -- TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)
        "com_insee"::text as "{{ sourceFieldPrefix }}com_code", -- NOT apcomsup_com_code because is OSM's version (which has been computed by geodatamine)
        "com_nom"::text as "{{ sourceFieldPrefix }}com_name" -- NOT apcomsup because is OSM's version (which has been computed by geodatamine)

    from source

),

parsed as (

    select
        *,
        {% if src_priority %}'{{ src_priority }}' || {% endif %}'{{ src_name }}' as "{{ fieldPrefix }}src_priority",  -- 0 is highest, then 10, 100, 1000... src_name added to differenciate
        uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns }}}}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}Id",
        ST_GeomFROMText('POINT(' || cast("X" as text) || ' ' || cast("Y" as text) || ')', 4326) as geometry, -- OU prefix ? forme ?? ou /et "Geom" ? TODO LATER s'en servir pour réconcilier si < 5m
        {{ schema }}.to_numeric_or_null("{{ fieldPrefix }}HauteurAppui__s") as "{{ fieldPrefix }}HauteurAppui" -- TODO Hauteur ! hauteur ? __m ??

    from renamed

),

translated as (

    select
        parsed.*,
        mat."Valeur" as "{{ fieldPrefix }}Materiau" -- TODO dict conv

    from parsed
        left join {{ ref('l_appuisaeriens_materiau_osmgeodatamine') }} mat -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on parsed.{{ sourceFieldPrefix }}material = mat.{{ sourceFieldPrefix }}material
            
    
)

select * from translated

{% endmacro %}