{#
Parsing of a priori (made-up), covering examples of the definition / interface.
Examples have to be **as representative** of all possible data as possible because they are also the basis of the definition.
For instance, for a commune INSEE id field, they should also include a non-integer value such as 2A035 (Belvédère-Campomoro).
Methodology :
1. copy the first line(s) from the specification document
2. add line(s) to contain further values for until they are covering for all columns
3. NB. examples specific to each source type are provided in _source_example along their implementation (for which they are covering)

TODO can't be replaced by from_csv because is the actual definition, BUT could be by guided by metamodel !
{{ apcom_equipement_from_csv(ref(model.name[:-4])) }}

#}

{{
  config(
    materialized="view"
  )
}}

{{ apcom_suivioccupation_from_csv() }}