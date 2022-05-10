{#
Enrichissement (par les communes) des données normalisées de toutes les sources de type appuiscommuns.supportaerien.

- on ne garde que les champs officiels

112s
#}

{% set fieldPrefix = 'apcomsup_' %}

with unioned as (
    select * from {{ ref("appuiscommuns_supportaerien") }}
),
linked as (
    {{ dedupe('unioned', id_fields=['"apcomsup_src_id"']) }}
)
select * from linked