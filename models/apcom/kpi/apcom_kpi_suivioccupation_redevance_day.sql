{#
Rajoute le calcul de la redevance à apcom_kpi_suivioccupation_day, de manière un peu différente donc séparée
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

), index_redevance as (
  select * from {{ ref('apcom_src_index_redevance_mois_parsed') }}

), suocc_day_pairs as (

  select
    -- dans le cas d'un suivi d'occupation d'un equipement de Nature Traverse, montant de la redevance associé à la traverse :
    case when "apcomoc_RefEquipement" is not null
        and ("apcomeq_Nature" = 'Traverse' or "apcomeq_Nature" = 'traverse')
        and "apcomsuoc_FinOccupation" > now() -- pas celles finies
        and (d.day = "apcomsuoc_DebutOccupation" or d.day = "apcomsuoc_FinOccupation") -- le versement de l'actuelle ou de son renouvellement
        then round(27.5*(0.15+85* (
            case when ir."Index" is NULL and d.day < now() then 106.2 when ir."Index" is NULL then 130 else cast(ir."Index" as numeric) end -- quand l'index n'est pas disponible, dans le passé on utilise 106.2 (i.e. ratio 1/1) et dans le futur 130
            )/106.2), 2) * (EXTRACT (YEAR FROM "apcomsuoc_FinOccupation") - EXTRACT (YEAR FROM "apcomsuoc_DebutOccupation"))/20 else 0 end -- règle de 3 si fin - debut <> 20 ans
        as "{{ fieldPrefixInd }}redevance",

    d.*,
    suocc.*

  from suocc, days d
  left join index_redevance ir on to_char(d.day, 'YYYY-MM') = ir."Libellé"

)
select
    *
    from suocc_day_pairs
    --order by day asc -- not needed for charts, only for dev