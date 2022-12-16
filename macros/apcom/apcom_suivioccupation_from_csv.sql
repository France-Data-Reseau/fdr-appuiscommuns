{#

Parsing of
- sources that are directly in the apcom types
- a priori (made-up), covering examples of the definition / interface.
Examples have to be **as representative** of all possible data as possible because they are also the basis of the definition.
For instance, for a commune INSEE id field, they should also include a non-integer value such as 2A035 (Belvédère-Campomoro).
Methodology :
1. copy the first line(s) from the specification document
2. add line(s) to contain further values for until they are covering for all columns
3. NB. examples specific to each source type are provided in _source_example along their implementation (for which they are covering)

TODO or _parsed ?
TODO can't be replaced by from_csv because is the actual definition, BUT could be by guided by metamodel !
{{ apcom_equipement_from_csv(ref(model.name[:-4])) }}

#}

{% macro apcom_suivioccupation_from_csv(source_model=ref(model.name | replace('_stg', ''))) %}

{% set fieldPrefix = 'apcomsuoc_' %}
{% set def_model = ref('apcom_def_suivioccupation_example') %}

select
    {{ dbt_utils.star(def_model,
        except=[fieldPrefix + 'RefOccupation', fieldPrefix + 'RefEquipement',
        fieldPrefix + 'DebutOccupation',
        fieldPrefix + 'Montant', fieldPrefix + 'DureeOccupation' ]) }},
    "{{ fieldPrefix }}RefOccupation", --::uuid, -- TODO Id or support it at parsing and out
    "{{ fieldPrefix }}RefEquipement", --::uuid,
    {{ schema }}.to_date_or_null("{{ fieldPrefix }}DebutOccupation"::text, 'YYYY/MM/DD HH24:mi:ss.SSS'::text,
          'YYYY-MM-DD"T"HH24:mi:ss.SSS'::text) as "{{ fieldPrefix }}DebutOccupation", -- 1987 https://www.ietf.org/rfc/rfc3339.txt
    {{ fdr_francedatareseau.to_numeric_or_null(fieldPrefix + "Montant", source_model) }} as "{{ fieldPrefix }}Montant",
    {{ fdr_francedatareseau.to_numeric_or_null(fieldPrefix + "DureeOccupation", source_model) }} as "{{ fieldPrefix }}DureeOccupation"

    from {{ source_model }}

{% endmacro %}