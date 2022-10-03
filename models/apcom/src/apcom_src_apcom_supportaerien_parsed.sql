{#
Generically parsed
#}

{{
  config(
    materialized="view",
  )
}}

{% set use_case_prefix = 'apcom' %}
{% set FDR_SOURCE_NOM = this.name | replace(use_case_prefix ~ '_src_', '') | replace('_parsed', '') | replace('_dict', '') %}
{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

{{ fdr_francedatareseau.fdr_source_union_from_name("supportaerien",
    has_dictionnaire_champs_valeurs,
    this,
    def_model=ref(use_case_prefix + '_def_' + FDR_SOURCE_NOM.replace(use_case_prefix + '_', '') + '_definition'),
    srid='4326') }}