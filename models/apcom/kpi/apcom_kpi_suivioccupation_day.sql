{#
Indicateurs métier d'occupation, par région - enriched day pairs, ready to be aggregated at whichever geo level as indicators

TODO :
voir README

possible macro params :
- TODO start_date ? end_date ??

du JDB :

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
  --select * from {{ source_or_test_ref('TODO', 'apcom_std_suivioccupation') }}
  select * from {{ ref('apcom_std_suivioccupation_unified') }}

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
  where d.day between suocc."apcomsuoc_DebutOccupation" and (d.day + suocc."apcomsuoc_DureeOccupation")

), suocc_day_pairs_enriched as (
select
    suoccd.*,
    occ.*,
    sup.*,
    -- TODO AODE
    com.com_code,
    com.com_name,
    com.epci_code,
    com.epci_name,
    com.dep_code,
    com.dep_name,
    com.reg_code,
    com.reg_name

    from suocc_day_pairs suoccd
        join {{ ref('apcom_std_occupation_unified') }} occ on suoccd."apcomsuoc_RefOccupation" = occ."apcomoc_IdOccupation"
        join {{ ref('apcom_std_equipement_unified') }} eq on occ."apcomoc_RefEquipement" = eq."apcomeq_IdEquipement"
        join {{ ref('apcom_std_supportaerien_unified') }} sup on eq."apcomeq_RefSupportAerien" = sup."apcomsup_Id"
        left join {{ ref('apcom_std_supportaerien_fdrcommune_linked') }} supcom -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on sup."apcomsup_Id" = supcom."apcomsup_Id" -- on sup."com_code" = supcom."com_code"
        -- no need to join also to commune, because the very common fields we require have already been included in the link table :
        left join {{ source('france-data-reseau', 'georef-france-commune.csv') }} com -- LEFT join sinon seulement les lignes qui ont une valeur !!
                on supcom.com_code = com.com_code
    -- TODO aode
    -- add reg_code and commune in _enriched NOOO using "specific" enriched : apcom_supportaerien_fdrcommune_linked

)
select
    *
    from suocc_day_pairs_enriched
    order by day asc