{#
Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source d'exemple embarquée "echantillon 3"

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la __definition) ?

assuming no need for exact dedup by src_id or geometry

#}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['"appuiscommunssupp__Id"']},
      {'columns': ['geometry'], 'type': 'gist'},]
  )
}}

{% set fieldPrefix = "appuiscommunssupp__" %}

-- TODO plug megalis as gthdv2 source type ? :
{% set sourceModels = [source_or_test_ref('gthdv2 megalis', 'megalis')] %}

with translated as (
    {% for sourceModel in sourceModels %}
    {{ gthdv2__apcom_supportaerien(sourceModel) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
){#, deduped_computed as (
    not generic so rather in specific macro above
    {{ apcom_supportaerien__deduped_computed('translated', fieldPrefix) }}
)#}
select * from translated
