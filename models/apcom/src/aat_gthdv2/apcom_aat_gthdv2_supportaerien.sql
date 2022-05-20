{#
Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source d'exemple embarquée "echantillon 3"

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la _definition) ?

assuming no need for exact dedup by src_id or geometry

#}

{% set fieldPrefix = "apcomsup_" %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"' + fieldPrefix + 'Id"']},
      {'columns': order_by_fields},
      {'columns': ['geometry'], 'type': 'gist'},]
  )
}}

-- TODO plug megalis as gthdv2 source type ? :
{% set sourceModels = [source_or_test_ref('appuiscommuns', 'apcom_aat_gthdv2_supportaerien')] %}

with translated as (
    -- TODO draft for several sources of the same type :
    -- TODO & add src_priority (& _name) here ?! in general ASAP (NOT after unification in source type) and NOT in same dbt model as dedup if any !
    {% for sourceModel in sourceModels %}
    {{ gthdv2__apcom_supportaerien(sourceModel) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
){# NO NEED for dedup within nor among soures (for "among", would have to be in a downstream model below src_name/priority indexes)
, deduped_computed as (
    not generic so rather in specific macro above
    {{ apcom_supportaerien__deduped_computed('translated', fieldPrefix) }}
)#}
select *--,
    --ST_Transform(geometry, 2154) as geometry_2154 -- Lambert 93 RATHER in union
from translated
order by "{{ order_by_fields | join('" asc, "') }}" asc
