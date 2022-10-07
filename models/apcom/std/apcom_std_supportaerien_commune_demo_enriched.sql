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
    apcomsup.*,
    -- com :
    {#{ dbt_utils.star(source('france-data-reseau', 'fdr_src_communes_ods'),
        except=['geometry'] + fdr_francedatareseau.list_import_fields(), relation_alias='com') }},#} -- _id is most probably added by CKAN to all imports
    com.geometry as com_geometry,
    com.com_code,
    com.com_name,
    com.epci_code,
    com.epci_name,
    com.dep_code,
    com.dep_name,
    com.reg_code,
    com.reg_name
    -- demo :
    {#{ dbt_utils.star(source('france-data-reseau', 'fdr_src_demographie_communes_2014_typed'),
        except=fdr_francedatareseau.list_import_fields()) }#} -- _id is most probably added by CKAN to all imports
    --, demo."Population"
    , com."Population"

    from {{ source_model }} apcomsup

        left join {{ ref('apcom_std_supportaerien_commune_linked') }} supcom -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on apcomsup."apcomsup_id" = supcom."apcomsup_id" -- on sup."com_code" = supcom."com_code"
        -- no need to join also to commune, because the very common fields we require have already been included in the link table NO NOT ALL :
        left join {{ source('france-data-reseau', 'fdr_std_communes_ods') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on supcom.com_code = com.com_code
)
select * from enriched