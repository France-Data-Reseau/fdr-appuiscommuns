{#
Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source d'exemple embarquée "echantillon 3"

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la __definition) ?

assuming no need for exact dedup by src_id or geometry
#}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"appuiscommunssupp__Id"']},
      {'columns': ['geometry'], 'type': 'gist'},]
  )
}}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'apcom_birdz_deftest' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'birdz' %} -- ?
{% set prefix = 'appuiscommunssupp' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '__' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{% set sourceModel = source_or_test_ref('birdz', 'birdz_supportaerien') %} -- TODO raw_

with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ sourceModel }}
    {% if var('limit', 0) > 0 %}
    LIMIT {{ var('limit') }}
    {% endif %}

),

renamed as (

    select
        '{{ sourceModel }}' as "{{ fieldPrefix }}src_name", -- source name (else won't have it anymore once unified with other sources)
        --id as "{{ fieldPrefix }}src_index", -- index in source
        replace("ADR_POS_X,C,254"::text, ',', '.')::float as "x", -- from French number format
        replace("ADR_POS_Y,C,254"::text, ',', '.')::float as "y", -- from French number format
        "PDR_NUM,C,254"::text as "{{ fieldPrefix }}src_id", -- source own id
        "POSE_DATE,C,254"::text as "{{ sourceFieldPrefix }}DateConstruction__s", -- 15/10/2021 TODO Q Date Construction ??
        "ADR_NUM_VO,C,254"::text as "{{ sourceFieldPrefix }}ADR_NUM_VO", -- 999 (text because could be 1bis)
        "ADR_NOM_RU,C,254"::text as "{{ sourceFieldPrefix }}ADR_NOM_RU", -- ROUTE DE KERGONAN
        split_part("PDR_NUM,C,254"::text, '_'::text, 1) as "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        split_part("PDR_NUM,C,254"::text, '_'::text, 1) as "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        split_part("PDR_NUM,C,254"::text, '_'::text, 1) as "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        "COMMUNE,C,254"::text as "{{ fieldPrefix }}commune_nom", -- ERGUE GABERIC enrichissement mminimal pour rendre code insee lisible ?
        "COMMUNE,C,254"::text as "{{ fieldPrefix }}fdrcommune__nom", -- ERGUE GABERIC TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)
        "CONTRAT_VE,C,254"::text as "{{ sourceFieldPrefix }}CONTRAT_VE", -- ERGUE GABERIC
        "TYPE_SUPPO,C,254"::text as "{{ sourceFieldPrefix }}TYPE_SUPPO", -- POTEAU ELECTRIQUE BETON
        "TYPE_EQUIP,C,254"::text as "{{ sourceFieldPrefix }}TYPE_EQUIP" -- RU
        --"nature"::text as "{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        --"operator"::text as "{{ fieldPrefix }}Gestionnaire",
        --NULL as "{{ fieldPrefix }}HauteurAppui",
        --NULL as "{{ fieldPrefix }}CodeExterne"
        -- TODO autocompléter colonnes ! NON requiert UNION avec star donc pas dans translated

    from source

),

parsed as (

    select
        "{{ fieldPrefix }}src_name",
        --"{{ fieldPrefix }}src_index",
        "{{ fieldPrefix }}src_id",
        uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns }}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}Id",
        ST_GeomFROMText('POINT(' || cast("x" as text) || ' ' || cast("y" as text) || ')', 4326) as geometry, -- OU geo_point__4326 ? prefix ?? forme ?? ou /et "Geom" ? TODO LATER s'en servir pour réconcilier si < 5m
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}DateConstruction__s", 'DD/MM/YY'::text) as "{{ fieldPrefix }}DateConstruction",
        "{{ sourceFieldPrefix }}ADR_NUM_VO", -- 999
        "{{ sourceFieldPrefix }}ADR_NOM_RU", -- ROUTE DE KERGONAN
        --"{{ sourceFieldPrefix }}utility", -- power
        --"{{ fieldPrefix }}Gestionnaire",
        --"appuiscommuns".to_numeric_or_null("{{ fieldPrefix }}HauteurAppui__s") as "{{ fieldPrefix }}HauteurAppui", -- TODO Hauteur ! hauteur ? __m ??
        --"{{ fieldPrefix }}HauteurAppui__s",
        --"{{ fieldPrefix }}CodeExterne", -- 101, 87, 37081ER073...
        "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        "{{ fieldPrefix }}commune_nom", -- enrichissement mminimal pour rendre code insee lisible ?
        "{{ fieldPrefix }}fdrcommune__nom", -- TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)
        "{{ sourceFieldPrefix }}CONTRAT_VE", -- ERGUE GABERIC
        "{{ sourceFieldPrefix }}TYPE_SUPPO", -- POTEAU ELECTRIQUE BETON
        "{{ sourceFieldPrefix }}TYPE_EQUIP" -- RU

    from renamed

),

translated as (

    select
        "{{ fieldPrefix }}src_name",
        --"{{ fieldPrefix }}src_index",
        "{{ fieldPrefix }}src_id",
        "{{ fieldPrefix }}Id",
        geometry, -- OU geo_point_4326 ? prefix ?? forme ??  ou /et "Geom" ?
        "{{ fieldPrefix }}DateConstruction",
        --"{{ sourceFieldPrefix }}utility", -- power
        --"{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        --"{{ fieldPrefix }}Gestionnaire",
        {{ ref('l_appuisaeriens_materiau__' + sourcePrefix) }}."Valeur" as "{{ fieldPrefix }}Materiau", -- TODO dict conv
        --"{{ fieldPrefix }}HauteurAppui", -- TODO Hauteur ! hauteur ? __m ??
        --"{{ fieldPrefix }}HauteurAppui__s",
        --"{{ fieldPrefix }}CodeExterne", -- 101, 87, 37081ER073...
        "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        "{{ fieldPrefix }}commune_nom", -- enrichissement mminimal pour rendre code insee lisible ?
        "{{ fieldPrefix }}fdrcommune__nom", -- TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)
        "{{ sourceFieldPrefix }}CONTRAT_VE", -- ERGUE GABERIC
        parsed."{{ sourceFieldPrefix }}TYPE_SUPPO", -- POTEAU ELECTRIQUE BETON ; parsed. else column reference "birdz__TYPE_SUPPO" is ambiguous
        "{{ sourceFieldPrefix }}TYPE_EQUIP" -- RU

    from parsed
        left join {{ ref('l_appuisaeriens_materiau__' + sourcePrefix) }} -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on parsed."{{ sourceFieldPrefix }}TYPE_SUPPO" = {{ ref('l_appuisaeriens_materiau__' + sourcePrefix) }}."{{ sourceFieldPrefix }}TYPE_SUPPO"

), computed as (
    {{ apcom_supportaerien_translation__computed("translated") }}
)

select * from computed
