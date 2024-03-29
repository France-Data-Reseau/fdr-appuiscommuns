{#

#}

{{
  config(
    materialized="view",
  )
}}

{% set use_case_prefix = var('use_case_prefix', var('FDR_CAS_USAGE')) %}
{% set FDR_SOURCE_NOM = this.name | replace(use_case_prefix ~ '_src_', '') | replace('_parsed', '') | replace('_dict', '') %}
{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

with imported as (
{{ fdr_francedatareseau.fdr_source_union_from_name(FDR_SOURCE_NOM,
    has_dictionnaire_champs_valeurs,
    this,
    srid='4326') }}
{# , best_geometry_columns=['geom', 'geometrie'], target_geometry_column_name='geometry' #}
)

select
    "{{ schema }}".to_numeric_or_null("Index") as "Index__numeric",
    *
from imported