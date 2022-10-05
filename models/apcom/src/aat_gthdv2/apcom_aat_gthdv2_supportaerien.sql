{#
_translated step
Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source d'exemple embarquée "echantillon 3"

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la _definition) ?

assuming no need for exact dedup by src_id or geometry

    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'IdSupportAerien"']},
      {'columns': order_by_fields},
      {'columns': ['geometry'], 'type': 'gist'},
    ]

#}

{% set fieldPrefix = "apcomsup_" %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="view",
  )
}}

-- TODO plug megalis as gthdv2 source type ? :
{% set sourceModel = ref('apcom_src_apcom_aat_gthdv2_parsed') if not var('use_example') else ref('apcom_aat_gthdv2_supportaerien_example_stg') %}

with imported as (
    select * from {{ sourceModel }}

), translated as (
    -- TODO & add src_priority (& _name) here ?! in general ASAP (NOT after unification in source type) and NOT in same dbt model as dedup if any !
    {{ gthdv2__apcom_supportaerien('imported') }}

){# NO NEED for dedup within nor among sources (for "among", would have to be in a downstream model below src_name/priority indexes)
, deduped as (
    {{ apcom_supportaerien_deduped('translated', fieldPrefix) }}
)#}
select *--,
    --ST_Transform(geometry, 2154) as geometry_2154 -- Lambert 93 RATHER in union
from translated
order by "{{ order_by_fields | join('" asc, "') }}" asc
