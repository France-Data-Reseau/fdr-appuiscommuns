{#
Indicateurs métier d'évolution d'occupation, agrégés par collectivité, commune

Indicateurs métier d'évolution journalière (mais agrégeable dans Superset
ex. annuellement pour redevance) sur suivioccupation (fin occupation, redevance traverse)

possible macro params :
- TODO start_date ? end_date ??

du JDB :

déclinés à différentes échelles géographiques : départementales, périmètre territorial de compétence du cas d'usage, communes.

Décompte total, segmenté par matériau
Décompte total, segmenté par exploitant électrique
Décompte total, segmenté par occupant télécoms , technologie et cheminement
=>
exploitant électrique => apcomsup_Gestionnaire
occupant télécoms => apcomoc_Gestionnaire
technologie => apcomoc_Technologie
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
  select *
  from {{ ref('apcom_std_suivioccupation_enriched') }}
  where "apcomsuoc_DebutOccupation" is not null -- else can't be used

), occupation_date_range as (

  select
    min("apcomsuoc_DebutOccupation") as start_date,
    max("apcomsuoc_FinOccupation") as end_date
  from suocc
  -- where meaningful for chosen indicators...

), days as (
  -- https://stackoverflow.com/questions/14113469/generating-time-series-between-two-dates-in-postgresql
  SELECT dd::date as day -- keeps up to the day
  FROM generate_series((select start_date from occupation_date_range limit 1)::timestamp,
      (select end_date from occupation_date_range limit 1)::timestamp, '1 day'::interval) dd

), suocc_day_pairs as (

  select
    -- fin occupation :
    case when "apcomsuoc_FinOccupation" < now() then 1 else 0 end as {{ fieldPrefixInd }}expire,
    case when "apcomsuoc_FinOccupation" BETWEEN now() and now() + INTERVAL '1 year' then 1 else 0 end as {{ fieldPrefixInd }}expire_avant_1_an,
    case when "apcomsuoc_FinOccupation" - now() BETWEEN INTERVAL '1 years 1 day' and INTERVAL '3 years' then 1 else 0 end as {{ fieldPrefixInd }}expire_avant_3_ans,
    case when "apcomsuoc_FinOccupation" - now() BETWEEN INTERVAL '3 years 1 day' and INTERVAL '5 years' then 1 else 0 end as {{ fieldPrefixInd }}expire_avant_5_ans,

    -- dans le cas d'un suivi d'occupation d'un equipement de Nature Traverse, montant de la redevance associé à la traverse :
    case when "apcomoc_RefEquipement" is not null and ("apcomeq_Nature" = 'Traverse' or "apcomeq_Nature" = 'traverse') and "apcomsuoc_FinOccupation" > now()
        then round(27.5*(0.15+85*130/106.2)/365, 2) else 0 end as "{{ fieldPrefixInd }}redevance",

    *
  from suocc, days d
  where d.day between suocc."apcomsuoc_DebutOccupation" and suocc."apcomsuoc_FinOccupation"

)
select
    *
    from suocc_day_pairs
    --order by day asc -- not needed for charts, only for dev