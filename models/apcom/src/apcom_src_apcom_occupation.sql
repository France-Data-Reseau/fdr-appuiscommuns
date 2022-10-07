{#
_translated step
adds generic fields (else _src_id/priority and id/uuid NULL), replacing the source's if it provided them
#}

{{
  config(
    materialized="view",
  )
}}

{% set field_prefix = "apcomoc_" %}
{% set fdr_namespace = 'occupation.' + var('fdr_namespace') %} -- ?

{% set source_model = ref('apcom_src_apcom_occupation_parsed') %}

with parsed as (
  select
    {{ dbt_utils.star(source_model, except=fdr_francedatareseau.list_generic_fields(field_prefix)) }},
    "{{ field_prefix }}IdOccupation" as {{ field_prefix }}src_id
  from {{ source_model }}
  {% if var('limit', 0) > 0 %}
  LIMIT {{ var('limit') }}
  {% endif %}

), with_generic_fields as (
    {{ fdr_francedatareseau.add_generic_fields('parsed', field_prefix, fdr_namespace, src_priority=None) }}
)
select * from with_generic_fields