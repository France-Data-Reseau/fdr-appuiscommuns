{#

NOT USED ?

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
{{ apcom_occupation_from_csv(ref(model.name[:-4])) }}

#}

{% macro apcom_occupation_from_csv(source_model) %}

{% set fieldPrefix = 'apcomoc_' %}
{% set def_model = ref('apcom_occupation__example') %}

select
    {{ dbt_utils.star(def_model,
        except=[fieldPrefix + 'IdOccupation', fieldPrefix + 'RefEquipement']) }},
    "{{ fieldPrefix }}IdOccupation"::uuid, -- TODO Id or support it at parsing and out
    "{{ fieldPrefix }}RefEquipement"::uuid

    from {{ source_model }}

{% endmacro %}