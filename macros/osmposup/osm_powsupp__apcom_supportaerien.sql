{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type supportaerien
de la source "osmgeodatamine_powersupports"
Partie spécifique à la source

Inclus computed (partagé en macro), ainsi qu'add_generic_fields()

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la __definition) ?
#}

{% macro osm_powsupp__apcom_supportaerien(parsed_source_relation, src_priority=None) %}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien_osmgeodatamine_powersupports_extract' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set fdr_namespace = 'supportaerien.' + var('fdr_namespace') %} -- ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'osmposup' %} -- ?
{% set prefix = 'apcomsup' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '_' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '_' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

with import_parsed as (

    select * from {{ parsed_source_relation }}
    {% if var('limit', 0) > 0 %}
    LIMIT {{ var('limit') }}
    {% endif %}

{#
rename and generic parsing is rather done
- in specific _from_csv
- in generic from_csv (called by fdr_source_union), which is guided by the previous one
#}

), renamed as (

    select
        *, -- TODO if test to debug / find conversion root error, else source('france-data-reseau', 'fdr_def_generic_fields_definition')

        --id as "{{ fieldPrefix }}src_index", -- index in source
        "osm_id"::text as "{{ fieldPrefix }}src_id", -- source own id
        "osm_id"::text as "{{ fieldPrefix }}IdSupportAerien", -- source own id
        "X"::numeric as "{{ sourceFieldPrefix }}X",
        "Y"::numeric as "{{ sourceFieldPrefix }}Y",
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

    from import_parsed

), specific_parsed as (

    select
        *,
        --uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns }}}}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}IdSupportAerien", -- NOO rather _id generic uuid in add_generic_fields() else not consistent between "source" and "echange" formats
        ST_GeomFROMText('POINT(' || cast("{{ sourceFieldPrefix }}X" as text) || ' ' || cast("{{ sourceFieldPrefix }}Y" as text) || ')', 4326) as geometry, -- OU prefix ? forme ?? ou /et "Geom" ? TODO LATER s'en servir pour réconcilier si < 5m
        {{ schema }}.to_numeric_or_null("{{ fieldPrefix }}HauteurAppui__s") as "{{ fieldPrefix }}HauteurAppui" -- TODO Hauteur ! hauteur ? __m ??

    from renamed

), translated as (

    select
        specific_parsed.*,
        --ST_TRANSFORM(geometry, 2154) as geometry_2154, -- TODO LATER preferred projection of data (data_owner_id's, or perimetre data_owner_id's or commune's but then have to reconcile with them first ? in from_csv ?!?)

        mat."Valeur" as "{{ fieldPrefix }}Materiau" -- TODO dict conv

    from specific_parsed
        left join {{ ref('l_appuisaeriens_materiau_osmgeodatamine') }} mat -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on specific_parsed.{{ sourceFieldPrefix }}material = mat.{{ sourceFieldPrefix }}material

), computed as (
    {{ apcom_supportaerien_translation__computed('translated') }}

), with_generic_fields as (
    {{ fdr_francedatareseau.add_generic_fields('computed', fieldPrefix, fdr_namespace, src_priority) }}

)

select * from with_generic_fields

{% endmacro %}