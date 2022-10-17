{#
Indicateurs métier d'occupation, agrégés par collectivité, commune

Version agrégée des indicateurs métier sur suivioccupation (fin occupation, redevance traverse)
par commune et gestionnaire / data_owner_id (rajouter dépose cuivre, déploiement fibre,
pivots selon materiau, exploitant, occupant, technologie, réseau convention)

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

{% set fieldPrefixSup = 'apcomsup_' %}
{% set fieldPrefixInd = 'apcomkpiocalert_' %}

{{
  config(
    materialized="view",
  )
}}

{% set source_model = ref('apcom_kpi_suivioccupation_alerte_fin') %}

with indicators as (
select

    -- AODE :
    "data_owner_id" as data_owner_id,
    MIN("data_owner_label") as data_owner_label,

    -- reg & com (outside geo) :
    com_code,
    MIN("com_name") as com_name,
    dep_code,
    MIN("dep_name") as dep_name,
    MIN("epci_code") as epci_code, -- a commune is only in one EPCI
    MIN("epci_name") as epci_name,
    MIN("reg_code") as reg_code,
    MIN("reg_name") as reg_name,

    count(*) as "{{ fieldPrefixInd }}all_count",

    -- fin occupation :
    SUM("{{ fieldPrefixInd }}expire") as "{{ fieldPrefixInd }}expire",
    SUM("{{ fieldPrefixInd }}expire_avant_1_an") as "{{ fieldPrefixInd }}expire_avant_1_an",
    SUM("{{ fieldPrefixInd }}expire_avant_3_ans") as "{{ fieldPrefixInd }}expire_avant_3_ans",
    SUM("{{ fieldPrefixInd }}expire_avant_5_ans") as "{{ fieldPrefixInd }}expire_avant_5_ans",

    -- redevance :
    SUM("{{ fieldPrefixInd }}redevance") as "{{ fieldPrefixInd }}redevance",

    -- déploiement fibre et dépose cuivre : (mais aussi dans les pivots)
    COUNT(*) filter (where "apcomoc_Technologie" = 'CUIVRE') as "{{ fieldPrefixInd }}cuivre_count",
    COUNT(*) filter (where "apcomoc_Technologie" = 'FIBRE') as "{{ fieldPrefixInd }}fibre_count",

    {{ dbt_utils.pivot('"' + fieldPrefixSup + 'Materiau"', dbt_utils.get_column_values(ref('apcom_std_supportaerien_unified'),
        '"' + fieldPrefixSup + 'Materiau"'), prefix=fieldPrefixSup + 'Materiau__') }},
    {{ dbt_utils.pivot('"apcomsup_Gestionnaire"', dbt_utils.get_column_values(ref('apcom_std_supportaerien_unified'),
        '"apcomsup_Gestionnaire"'), prefix='apcomsup_Gestionnaire') }},
    {{ dbt_utils.pivot('"apcomoc_Gestionnaire"', dbt_utils.get_column_values(ref('apcom_std_occupation_unified'),
        '"apcomoc_Gestionnaire"'), prefix='apcomoc_Gestionnaire') }},
    {{ dbt_utils.pivot('"apcomoc_Technologie"', dbt_utils.get_column_values(ref('apcom_std_occupation_unified'),
        '"apcomoc_Technologie"'), prefix='apcomoc_Technologie__') }},
    {{ dbt_utils.pivot('"apcomoc_Reseau"', dbt_utils.get_column_values(ref('apcom_std_occupation_unified'),
        '"apcomoc_Reseau"'), prefix='apcomoc_Reseau') }},

    {{ dbt_utils.pivot('"apcomsuoc_Convention"', dbt_utils.get_column_values(ref('apcom_std_suivioccupation_unified'),
        '"apcomsuoc_Convention"'), prefix='apcomsuoc_Convention') }}

    from {{ source_model }}
    --where "apcomsuoc_FinOccupation" > now() -- else get too old alerts NOO would remove those that missed the date !
    group by
        data_owner_id, com_code, dep_code -- in case a commune is in 2 departements
)

select
    indicators.*
    from indicators
    --order by day asc -- not needed for charts, only for dev