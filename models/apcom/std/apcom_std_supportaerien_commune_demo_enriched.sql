{#
Enrichissement (par les communes et données démographiques dont population) des données normalisées
de toutes les sources de type appuiscommuns.supportaerien.

Donc le bon point de départ sur tous les indicateurs sur les supports aeriens (et leurs enrichissements).

- on ne garde que les champs officiels
#}

{{
  config(
    materialized="view"
  )
}}

{# TODO {% set source_model = ref('apcom_std_supportaerien_deduped') %} #}
{% set source_model = ref('apcom_std_supportaerien_unified') %}

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
    {{ dbt_utils.star(source('france-data-reseau', 'fdr_src_demographie_communes_2014_typed'),
        except=fdr_francedatareseau.list_import_fields()) }} -- _id is most probably added by CKAN to all imports

    from {{ source_model }} apcomsup

        left join {{ ref('apcom_std_supportaerien_commune_linked') }} supcom -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on apcomsup."apcomsup_IdSupportAerien" = supcom."apcomsup_IdSupportAerien" -- on sup."com_code" = supcom."com_code"
        -- no need to join also to commune, because the very common fields we require have already been included in the link table NO NOT ALL :
        left join {{ source('france-data-reseau', 'fdr_src_communes_ods') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on supcom.com_code = com.com_code
    -- TODO OLD add reg_code and commune in _enriched NOOO using "specific" enriched : apcom_supportaerien_fdrcommune_linked

    left join {{ source('france-data-reseau', 'fdr_src_demographie_communes_2014_typed') }} demo -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        --on apcomsup."com_code" = demo."CODGEO"
        --on apcomsuparr."com_code__arr_u" = demo."CODGEO"
        on com.com_code = demo."CODGEO"
)
select * from enriched