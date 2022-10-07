{#
Enrichissement (par les communes et données démographiques dont population) des données normalisées
de toutes les sources de type appuiscommuns.suivioccupation.

Donc le bon point de départ sur tous les indicateurs sur les suivioccupation (et leurs enrichissements).

#}

{% set fieldPrefix = 'apcomsup' + '_' %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="view",
  )
}}


select
    suocc.*,
    {{ dbt_utils.star(ref('apcom_std_occupation_unified'), relation_alias="occ",
        except=fdr_francedatareseau.list_import_fields()) }},
    {# prefix="occ_",  #}
    {{ dbt_utils.star(ref('apcom_std_supportaerien_unified'), relation_alias="supen",
        except=fdr_francedatareseau.list_import_fields()) }},
    {# prefix="sup_",  #}
    {#
    --suocc.*,
    {{ dbt_utils.star(ref('apcom_def_occupation_definition'), relation_alias="occ",
        except=fdr_francedatareseau.list_import_fields()) }},
    -- equipement : (actually not used in kpi)
    {{ dbt_utils.star(ref('apcom_def_equipement_definition'), relation_alias="eq",
        except=fdr_francedatareseau.list_import_fields()) }},
    {{ dbt_utils.star(ref('apcom_def_supportaerien_definition'), relation_alias="sup",
        except=fdr_francedatareseau.list_import_fields()) }},
    #}

    -- AODE :
    -- already within above fields

    -- reg & com (outside geo) :
    -- already within above enriched fields
    supen.com_code,
    supen.com_name,
    supen.epci_code,
    supen.epci_name,
    supen.dep_code,
    supen.dep_name,
    supen.reg_code,
    supen.reg_name
    -- demo :
    , supen."Population"

    -- enrich with region : NOT NEEDED
    --,
    --region.geometry,
    --region.geometry_shape_2154

    from {{ ref('apcom_std_suivioccupation_unified') }} suocc
        -- TODO TODO join on fields unique across data_owner_id / FDR_SIREN !
        join {{ ref('apcom_std_occupation_unified') }} occ on suocc."apcomsuoc_RefOccupation" = occ."apcomoc_IdOccupation" and suocc.data_owner_id = occ.data_owner_id
        join {{ ref('apcom_std_equipement_unified') }} eq on occ."apcomoc_RefEquipement" = eq."apcomeq_IdEquipement" and occ.data_owner_id = eq.data_owner_id
        {#
        join {{ ref('apcom_std_supportaerien_unified') }} sup on eq."apcomeq_RefSupportAerien" = sup."apcomsup_IdSupportAerien" and eq.data_owner_id = sup.data_owner_id
        left join {{ ref('apcom_std_supportaerien_commune_linked') }} supcom -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on sup."apcomsup_id" = supcom."apcomsup_id" -- on sup."com_code" = supcom."com_code"

        -- no need to join to AODE, because no more of its fields are required.
        -- no need to join also to commune, because the very common fields we require have already been included in the link table NO NOT ALL :
        left join {{ source('france-data-reseau', 'fdr_src_communes_ods') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on supcom.com_code = com.com_code

        -- enrich with region : NOT NEEDED
        --left join {{ source('france-data-reseau','fdr_src_regions_ods') }} region --  LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        --    on "reg_code" = region."Code Officiel Région"
        #}
        join {{ ref('apcom_std_supportaerien_commune_demo_enriched') }} supen on eq."apcomeq_RefSupportAerien" = supen."apcomsup_IdSupportAerien" and eq.data_owner_id = supen.data_owner_id