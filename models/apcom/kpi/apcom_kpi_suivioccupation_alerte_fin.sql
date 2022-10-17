{#
Indicateurs métier d'occupation

Indicateurs métier sur suivioccupation (fin occupation, redevance traverse)

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
occupant télécoms => apcomoc_Gestionnaire
technologie => apcomoc_Technologie
cheminement => apcomoc_Reseau : DI (distribution), RA (raccordement) repris de gthdv2 ; soit le câble est de collecte (départemental), soit de transport (vers ville), soit DI RA on se rapproche de l'abonné
TODO conventions => label / code dans apcomsuoc_Convention
mise en page / ordonnancement : d'abord le découpage géographique, et après le découpage métier

Projection des échéances de fin d'occupation
Les occupations étant limitées dans le temps, on cherchera à prévenir l'utilisateur des quantités arrivant à échéance à 1, 3 ou 5 ans pour qu'il anticipe un renouvellement auprès des opérateurs concernés.
On représentera donc à l'aide d'un histogramme journalier la quantité de supports concernés par une échéance d'occupation pour chacune de ces dates. Cette représentation pourra être déclinée à différentes échelles géographique comme précédemment.

Suivi des typologies conventionnelles
On représentera donc sur histogramme l'évolution du nombre total d'appuis concernés par l'un ou l'autre des types conventionnels. Chaque catégorie étant empilée sur les autres sur l'histogramme. Le total pourra donner plus que le nombre total de poteaux existant : certains poteaux sont concernés par plusieurs conventions.
#}

{% set fieldPrefixInd = 'apcomkpiocalert_' %}

{{
  config(
    materialized="view",
  )
}}

with suocc as (
  select *
  from {{ ref('apcom_std_suivioccupation_enriched') }}
  where "apcomsuoc_DebutOccupation" is not null -- else can't be used

)

select
    -- fin occupation :
    case when "apcomsuoc_FinOccupation" < now() then 1 else 0 end as {{ fieldPrefixInd }}expire,
    case when "apcomsuoc_FinOccupation" BETWEEN now() and now() + INTERVAL '1 year' then 1 else 0 end as {{ fieldPrefixInd }}expire_avant_1_an,
    case when "apcomsuoc_FinOccupation" - now() BETWEEN INTERVAL '1 years 1 day' and INTERVAL '3 years' then 1 else 0 end as {{ fieldPrefixInd }}expire_avant_3_ans,
    case when "apcomsuoc_FinOccupation" - now() BETWEEN INTERVAL '3 years 1 day' and INTERVAL '5 years' then 1 else 0 end as {{ fieldPrefixInd }}expire_avant_5_ans,

    -- dans le cas d'un suivi d'occupation d'un equipement de Nature Traverse, montant de la redevance associé à la traverse :
    case when "apcomoc_RefEquipement" is not null and ("apcomeq_Nature" = 'Traverse' or "apcomeq_Nature" = 'traverse') and "apcomsuoc_FinOccupation" > now() then round(27.5*(0.15+85*130/106.2), 2) else 0 end as "{{ fieldPrefixInd }}redevance",

    *
from suocc
--where "apcomsuoc_FinOccupation" > now() -- else get too old alerts NOO would remove those that missed the date !