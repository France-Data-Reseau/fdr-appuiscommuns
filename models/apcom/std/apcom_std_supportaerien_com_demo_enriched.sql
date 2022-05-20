{#
Enrichissement (par les communes) des données normalisées de toutes les sources de type appuiscommuns.supportaerien.

TODO make it an enrichment specific to example indicators

- on ne garde que les champs officiels
#}

{{
  config(
    materialized="view"
  )
}}

{% set source_model = ref('apcom_std_supportaerien_commune_array_linked') %}

with enriched as (
{#
Alternative : implicit SELECT * or=dbt_utils.star(my_model_definition_relation) or all fields explicitly...
#}
select
    -- apcomsup :
    {{ dbt_utils.star(ref('apcom_def_supportaerien_definition')) }},
    -- com :
    {{ dbt_utils.star(source('france-data-reseau', 'georef-france-commune.csv'), except=['_id', '_full_text'], relation_alias='com') }}, -- _id is most probably added by CKAN to all imports
    -- demo :
    {{ dbt_utils.star(source('france-data-reseau', 'INSEE communes données démographiques'), except=['_id', '_full_text']) }} -- _id is most probably added by CKAN to all imports
    from {{ source_model }} apcomsup
    CROSS JOIN unnest(apcomsup."apcomsup_com_code__arr") apcomsuparr("apcomsup_com_code__arr_u")
    left join {{ source('france-data-reseau', 'georef-france-commune.csv') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on apcomsuparr."apcomsup_com_code__arr_u" = com.com_code
    left join {{ source('france-data-reseau', 'INSEE communes données démographiques') }} demo -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        --on apcomsup."com_code" = demo."CODGEO"
        --on apcomsuparr."com_code__arr_u" = demo."CODGEO"
        on com.com_code = demo."CODGEO"
)
select * from enriched