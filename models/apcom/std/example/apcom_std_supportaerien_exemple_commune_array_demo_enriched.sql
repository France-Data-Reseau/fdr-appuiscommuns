{#
DESACTIVE par défaut, gardé à titre d'exemple de array_linked

NOO OLD TABLE !!

Enrichissement (par les communes) des données normalisées de toutes les sources de type appuiscommuns.supportaerien.

TODO make it an enrichment specific to example indicators

- on ne garde que les champs officiels
#}

{{
  config(
    enabled=var("enableArrayLinked", false) | as_bool,
    materialized="view"
  )
}}

{% set source_model = ref('apcom_std_supportaerien_exemple_commune_array_linked') %}

with enriched as (
{#
Alternative : implicit SELECT * or=dbt_utils.star(my_model_definition_relation) or all fields explicitly...
#}
select
    -- apcomsup :
    {{ dbt_utils.star(ref('apcom_def_supportaerien_definition'), relation_alias='apcomsup') }},
    -- com :
    {{ dbt_utils.star(source('france-data-reseau', 'fdr_src_communes_ods'), except=['geometry'], relation_alias='com') }}, -- _id is most probably added by CKAN to all imports
    com.geometry as com_geometry,
    -- demo :
    {{ dbt_utils.star(source('france-data-reseau', 'fdr_src_demographie_communes_2014_typed')) }} -- _id is most probably added by CKAN to all imports
    from {{ source_model }} apcomsup
    CROSS JOIN unnest(apcomsup."apcomsup_com_code__arr") apcomsuparr("apcomsup_com_code__arr_u") -- TODO rather any() https://stackoverflow.com/questions/68418462/how-to-use-postgresql-array-in-where-in-clause
    left join {{ source('france-data-reseau', 'fdr_src_communes_ods') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on apcomsuparr."apcomsup_com_code__arr_u" = com.com_code
    left join {{ source('france-data-reseau', 'fdr_src_demographie_communes_2014_typed') }} demo -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        --on apcomsup."com_code" = demo."CODGEO"
        --on apcomsuparr."com_code__arr_u" = demo."CODGEO"
        on com.com_code = demo."CODGEO"
)
select * from enriched