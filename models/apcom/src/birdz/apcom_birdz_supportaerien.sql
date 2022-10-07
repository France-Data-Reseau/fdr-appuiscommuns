{#
_translated step
Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source d'exemple embarquée "echantillon 3"

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la _definition) ?

Inclus computed (partagé en macro), ainsi qu'add_generic_fields()

assuming no need for exact dedup by src_id or geometry

    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'id"']},
      {'columns': order_by_fields},
      {'columns': ['geometry'], 'type': 'gist'},]

#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'apcom_birdz_deftest' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set fdr_namespace = 'supportaerien.' + var('fdr_namespace') %} -- ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'birdzsup' %} -- ?
{% set prefix = 'apcomsup' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '_' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '_' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="view",
  )
}}

{% set sourceModel = ref('apcom_src_apcom_equip_birdz_parsed') if not (var('use_example') | as_bool) else ref('apcom_birdz_example_stg') %}

with source as (

    select * from {{ sourceModel }}
    {% if var('limit', 0) > 0 %}
    LIMIT {{ var('limit') }}
    {% endif %}

),

renamed as (

    select
        *, -- TODO if test to debug / find conversion root error, else source('france-data-reseau', 'fdr_def_generic_fields_definition')

        --id as "{{ fieldPrefix }}src_index", -- index in source
        "PDR_NUM,C,254"::text as "{{ fieldPrefix }}src_id", -- source own id
        "PDR_NUM,C,254"::text as "{{ fieldPrefix }}IdSupportAerien", -- source own id
        replace("ADR_POS_X,C,254"::text, ',', '.')::float as "x", -- from French number format
        replace("ADR_POS_Y,C,254"::text, ',', '.')::float as "y", -- from French number format
        "POSE_DATE,C,254"::text as "{{ sourceFieldPrefix }}DateConstruction__s", -- 15/10/2021 TODO Q Date Construction ??
        "ADR_NUM_VO,C,254"::text as "{{ sourceFieldPrefix }}ADR_NUM_VO", -- 999 (text because could be 1bis)
        "ADR_NOM_RU,C,254"::text as "{{ sourceFieldPrefix }}ADR_NOM_RU", -- ROUTE DE KERGONAN
        split_part("PDR_NUM,C,254"::text, '_'::text, 1) as "{{ sourceFieldPrefix }}com_code",
        "COMMUNE,C,254"::text as "{{ fieldPrefix }}com_name", -- ERGUE GABERIC enrichissement mminimal pour rendre code insee lisible ?
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
        *,
        --uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns }}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}IdSupportAerien", -- NOO rather in add_generic_fields()
        ST_GeomFROMText('POINT(' || cast("x" as text) || ' ' || cast("y" as text) || ')', 4326) as geometry, -- OU geo_point__4326 ? prefix ?? forme ?? ou /et "Geom" ? TODO LATER s'en servir pour réconcilier si < 5m
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}DateConstruction__s", 'DD/MM/YY'::text) as "{{ fieldPrefix }}DateConstruction"

    from renamed

),

translated as (

    select
        parsed.*,
        mat."Valeur" as "{{ fieldPrefix }}Materiau" -- TODO dict conv

    from parsed
        left join {{ ref('l_appuisaeriens_materiau') }} mat -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on parsed."{{ sourceFieldPrefix }}TYPE_SUPPO" = mat."{{ sourceFieldPrefix }}TYPE_SUPPO"

), computed as (
    {{ apcom_supportaerien_translation__computed("translated") }}

), with_generic_fields as (
    {{ fdr_francedatareseau.add_generic_fields('computed', fieldPrefix, fdr_namespace, src_priority) }}

)

select * from with_generic_fields
order by "{{ order_by_fields | join('" asc, "') }}" asc
