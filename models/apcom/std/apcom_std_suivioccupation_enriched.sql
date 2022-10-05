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
    {{ dbt_utils.star(ref('apcom_std_supportaerien_unified'), relation_alias="sup",
        except=fdr_francedatareseau.list_import_fields()) }},
    {# prefix="sup_",  #}

    -- AODE :
    -- already within above fields

    -- reg & com (outside geo) :
    com.com_code,
    com.com_name,
    com.epci_code,
    com.epci_name,
    com.dep_code,
    com.dep_name,
    com.reg_code,
    com.reg_name

    from {{ ref('apcom_std_suivioccupation_unified') }} suocc
        -- TODO TODO join on fields unique across data_owner_id / FDR_SIREN !
        join {{ ref('apcom_std_occupation_unified') }} occ on suocc."apcomsuoc_RefOccupation" = occ."apcomoc_IdOccupation"
        join {{ ref('apcom_std_equipement_unified') }} eq on occ."apcomoc_RefEquipement" = eq."apcomeq_IdEquipement"
        join {{ ref('apcom_std_supportaerien_unified') }} sup on eq."apcomeq_RefSupportAerien" = sup."apcomsup_IdSupportAerien"
        left join {{ ref('apcom_std_supportaerien_commune_linked') }} supcom -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on sup."apcomsup_IdSupportAerien" = supcom."apcomsup_IdSupportAerien" -- on sup."com_code" = supcom."com_code"

        -- no need to join to AODE, because no more of its fields are required.
        -- no need to join also to commune, because the very common fields we require have already been included in the link table NO NOT ALL :
        left join {{ source('france-data-reseau', 'fdr_src_communes_ods') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on supcom.com_code = com.com_code
    -- TODO OLD add reg_code and commune in _enriched NOOO using "specific" enriched : apcom_supportaerien_fdrcommune_linked