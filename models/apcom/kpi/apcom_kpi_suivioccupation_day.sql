{#
Indicateurs métier d'occupation, par région - enriched day pairs, ready to be aggregated at whichever geo level as indicators

TODO :
voir README

possible macro params :
- TODO start_date ? end_date ??

du JDB :

du JDB :

déclinés à différentes échelles géographiques : départementales, périmètre territorial de compétence du cas d'usage, communes.

Décompte total, segmenté par matériau
Décompte total, segmenté par exploitant électrique
Décompte total, segmenté par occupant télécoms , technologie et cheminement
=>
exploitant électrique => apcomsupp_Gestionnaire
technologie => apcomoc_Technologie
occupant télécoms => apcomoc_Gestionnaire
cheminement => apcomoc_Reseau : DI (distribution), RA (raccordement) repris de gthdv2 ; soit le câble est de collecte (départemental), soit de transport (vers ville), soit DI RA on se rapproche de l'abonné
TODO conventions => label / code dans apcomsuoc_Convention
mise en page / ordonnancement : d'abord le découpage géographique, et après le découpage métier

Suivi de la dépose du cuivre
Représentation en histogramme ou courbe de l'évolution du nombre total de supports occupés par un réseau téléphonique cuivre (SupportAerienOccupation.Technologie='CUIVRE'), d'après les dates d'occupation connues dans le modèle d'échange.
Cette représentation pourra être déclinée à différentes échelles géographique comme précédemment.

Suivi de la montée en charge des déploiements
Représentation en histogramme ou courbe de l'évolution du nombre total de supports occupés par un réseau fibre (SupportAerienOccupation.Technologie='FIBRE'), d'après les dates d'occupation connues dans le modèle d'échange.
Cette représentation pourra être déclinée à différentes échelles géographique comme précédemment.
#}

{% set fieldPrefixInd = 'apcomkpiocday_' %}

{{
  config(
    materialized="view",
  )
}}

with suocc as (
  {# select * from {{ source_or_test_ref('TODO', 'apcom_std_suivioccupation') }} #}
  select *,
    -- adding default apcomsuoc_DureeOccupation else removes those before 2018 (1950...) :
    ("apcomsuoc_DebutOccupation" + coalesce("apcomsuoc_DureeOccupation", 7300)) as "apcomsuoc_FinOccupation"
  from {{ ref('apcom_std_suivioccupation_enriched') }}
  where "apcomsuoc_DebutOccupation" is not null -- else can't be used

), occupation_date_range as (

  select min("apcomsuoc_DebutOccupation") as start_date, now() as end_date
  from suocc
  -- where meaningful for chosen indicators...

), days as (
  -- https://stackoverflow.com/questions/14113469/generating-time-series-between-two-dates-in-postgresql
  SELECT dd::date as day -- keeps up to the day
  FROM generate_series((select start_date from occupation_date_range limit 1)::timestamp,
      (select end_date from occupation_date_range limit 1)::timestamp, '1 day'::interval) dd

), suocc_day_pairs as (

  select *
  from suocc, days d
  where d.day between suocc."apcomsuoc_DebutOccupation" and suocc."apcomsuoc_FinOccupation"

{#
), suocc_day_pairs_enriched as (

select
    suoccd.*,
    -- list all fields except import fields that would conflict :
    {{ dbt_utils.star(ref('apcom_std_occupation_unified'), relation_alias="occ",
        except=fdr_francedatareseau.list_import_fields()) }},
    {# prefix="occ_", }
    {{ dbt_utils.star(ref('apcom_std_supportaerien_unified'), relation_alias="sup",
        except=fdr_francedatareseau.list_import_fields()) }},
    {# prefix="sup_",  }

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

    from suocc_day_pairs suoccd
        -- TODO TODO join on fields unique across data_owner_id / FDR_SIREN !
        join {{ ref('apcom_std_occupation_unified') }} occ on suoccd."apcomsuoc_RefOccupation" = occ."apcomoc_IdOccupation"
        join {{ ref('apcom_std_equipement_unified') }} eq on occ."apcomoc_RefEquipement" = eq."apcomeq_IdEquipement"
        join {{ ref('apcom_std_supportaerien_unified') }} sup on eq."apcomeq_RefSupportAerien" = sup."apcomsup_IdSupportAerien"
        left join {{ ref('apcom_std_supportaerien_fdrcommune_linked') }} supcom -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on sup."apcomsup_IdSupportAerien" = supcom."apcomsup_IdSupportAerien" -- on sup."com_code" = supcom."com_code"

        -- no need to join to AODE, because no more of its fields are required.
        -- no need to join also to commune, because the very common fields we require have already been included in the link table NO NOT ALL :
        left join {{ source('france-data-reseau', 'fdr_src_communes_ods') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on supcom.com_code = com.com_code
    -- add reg_code and commune in _enriched NOOO using "specific" enriched : apcom_supportaerien_fdrcommune_linked
#}

)
select
    *
    from suocc_day_pairs -- suocc_day_pairs_enriched
    --order by day asc -- not needed for charts, only for dev