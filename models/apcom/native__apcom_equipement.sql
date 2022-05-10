{#
Unification des données normalisées de toutes les sources de type appuiscommuns.supportaerien
#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set typeName = 'SupportAerien' %}
{% set prefix = 'appuiscommunssupp' %} -- ?
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{#
Union using dbt_utils helper :
- _definition (with 0 data lines) as the first unioned relation adds even fields missing in all normalizations, with the right type,
if they are provided in the official type definition
- include=dbt_utils.star(_definition) excludes source-specific fields
- source_column_name="_dbt_source_relation"
-

is a table only if has reconciliation
    include=dbt_utils.star(ref('appuiscommuns_supportaerien__definition')),
#}

{{
  config(
    materialized="view"
  )
}}

{% set sources = ['apcom_equipement'] %}
{% set source_relations = [] %}
{% for source in sources %}
  -- {{ source_relations.append(source_or_test_ref('TODO', source)) }} TODO source_or_test_ref()
{% endfor %}

with unioned as (

{{ dbt_utils.union_relations(relations=([
      ref('apcom_equipement_definition')]
      + source_relations))
}}

)

select * from unioned