{#
Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source d'exemple embarquée "echantillon 3"

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la _definition) ?

assuming no need for exact dedup by src_id or geometry
#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'apcom_birdz_deftest' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_equipement' %} -- _2021 ? from this file ? prefix:typeName ?
{% set ns_apcomeq = 'equipement.appuiscommuns.francedatareseau.fr' %} -- ?
{% set ns_apcomsup = 'equipement.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'birdzeq' %} -- ?
{% set prefix = 'apcomeq' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '_' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '_' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'IdEquipement"']},
      {'columns': order_by_fields},
      ]
  )
}}

{% set sourceModel = source_or_test_ref('appuiscommuns', 'apcom_birdz_supportaerien') %} -- TODO raw_

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
        split_part("PDR_NUM,C,254"::text, '_'::text, 1) as "apcomsup_com_code",
        "COMMUNE,C,254"::text as "apcomsup_com_name", -- ERGUE GABERIC enrichissement mminimal pour rendre code insee lisible ?
        "CONTRAT_VE,C,254"::text as "{{ sourceFieldPrefix }}CONTRAT_VE", -- ERGUE GABERIC TODO apcomeq__Proprietaire/Gestionnaire ?
        "TYPE_EQUIP,C,254"::text as "{{ sourceFieldPrefix }}TYPE_EQUIP" -- RU TODO apcomeq__Nature !
        -- missing : apcomeq__Hauteur	apcomeq__Proprietaire	apcomeq__Gestionnaire	apcomeq__Nature	apcomeq__NatureTraverse
        -- autocompléter colonnes ? NON requiert UNION avec star donc pas dans translated

    from source

),

parsed as (

    select
        *,
        {% if src_priority %}'{{ src_priority }}' || {% endif %}'{{ src_name }}' as "{{ fieldPrefix }}src_priority",  -- 0 is highest, then 10, 100, 1000... src_name added to differenciate
        uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns_apcomeq }}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}IdEquipement",
        uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns_apcomsup }}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}RefSupportAerien"

    from renamed

),

translated as (

    select
        parsed.*,
        eq."Valeur" as "{{ fieldPrefix }}Nature" -- TODO dict conv

    from parsed
        left join {{ ref('l_appuisaeriens_equipement') }} eq -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on parsed."{{ sourceFieldPrefix }}TYPE_EQUIP" = eq."{{ sourceFieldPrefix }}TYPE_EQUIP"

)
-- no computed

select * from translated
order by "{{ order_by_fields | join('" asc, "') }}" asc
