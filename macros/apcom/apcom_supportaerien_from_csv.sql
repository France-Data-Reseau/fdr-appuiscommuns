{#

TODO USED ?

Parsing of
- sources that are directly in the apcom types
- a priori (made-up), covering examples of the definition / interface.
- test _expected
Examples have to be **as representative** of all possible data as possible because they are also the basis of the definition.
For instance, for a commune INSEE id field, they should also include a non-integer value such as 2A035 (Belvédère-Campomoro).
Methodology :
1. copy the first line(s) from the specification document
2. add line(s) to contain further values for until they are covering for all columns
3. NB. examples specific to each source type are provided in _source_example along their implementation (for which they are covering)

TODO or _parsed ?
TODO can't be replaced by from_csv because is the actual definition, BUT could be by guided by metamodel !
{{ apcom_supportaerien_from_csv(ref(model.name[:-4])) }}
#}

{% macro apcom_supportaerien_from_csv(source_model=ref(model.name | replace('_stg', ''))) %}

{% set fieldPrefix = 'apcomsup_' %}
{% set def_model = ref('apcom_def_supportaerien_example') %}

select
    {{ dbt_utils.star(def_model,
        except=[fieldPrefix + 'Id', 'geometry', fieldPrefix + 'HauteurAppui', fieldPrefix + 'HauteurTotal',
        fieldPrefix + 'Azimut', fieldPrefix + 'DateConstruction', fieldPrefix + 'EffortTransversal',
        fieldPrefix + 'RemonteeAerosout', fieldPrefix + 'BoisCreosote', fieldPrefix + 'BandeauVert']) }},
    "{{ fieldPrefix }}Id"::uuid,
    ST_GeomFROMText(geometry, 4326) as geometry, -- NOT ::geometry else not the same (srid ?? only visible in binary ::text form : ) therefore except does not work
    -- 0101000000197B8A77DBE0E33F18C25725ECC34740 expected
    -- 0101000020E6100000197B8A77DBE0E33F18C25725ECC34740 actual
    {{ to_numeric_or_null(fieldPrefix + "HauteurAppui", source_model) }} as "{{ fieldPrefix }}HauteurAppui",
    {{ to_numeric_or_null(fieldPrefix + "HauteurTotal", source_model) }} as "{{ fieldPrefix }}HauteurTotal",
    {{ to_numeric_or_null(fieldPrefix + "Azimut", source_model) }} as "{{ fieldPrefix }}Azimut",
    {{ schema }}.to_date_or_null("{{ fieldPrefix }}DateConstruction"::text, 'YYYY/MM/DD HH24:mi:ss.SSS'::text,
      'YYYY-MM-DDTHH24:mi:ss.SSS'::text) as "{{ fieldPrefix }}DateConstruction", -- 1987 https://www.ietf.org/rfc/rfc3339.txt
    {{ to_numeric_or_null(fieldPrefix + "EffortTransversal", source_model) }} as "{{ fieldPrefix }}EffortTransversal",
    {{ schema }}.to_boolean_or_null("{{ fieldPrefix }}RemonteeAerosout") as "{{ fieldPrefix }}RemonteeAerosout",
    {{ schema }}.to_boolean_or_null("{{ fieldPrefix }}BoisCreosote") as "{{ fieldPrefix }}BoisCreosote",
    {{ schema }}.to_boolean_or_null("{{ fieldPrefix }}BandeauVert") as "{{ fieldPrefix }}BandeauVert"
    -- TODO rm :
    ----'"datastore"."appuiscommuns"."osmgeodatamine_powsupp__appuiscommuns_supportaerien"' as _dbt_source_relation,
    --appuiscommunssupp__fdrcommune__insee_id as appuiscommunssupp__commune_insee_id,
    ----appuiscommunssupp__fdrcommune__insee_id as fdrcommune__insee_id
    --appuiscommunssupp__fdrcommune__nom as appuiscommunssupp__commune_nom

    from {{ source_model }}

{% endmacro %}