{#

TODO WRONG REPLACE

Parsing of a priori (made-up), covering examples of the definition / interface.
Examples have to be **as representative** of all possible data as possible because they are also the basis of the definition.
For instance, for a commune INSEE id field, they should also include a non-integer value such as 2A035 (Belvédère-Campomoro).
Methodology :
1. copy the first line(s) from the specification document
2. add line(s) to contain further values for until they are covering for all columns
3. NB. examples specific to each source type are provided in _source_example along their implementation (for which they are covering)

TODO can't be replaced by from_csv because is the actual definition, BUT could be by guided by metamodel !
{{ apcom_supportaerien_from_csv(ref(model.name[:-4])) }}

#}

{{
  config(
    materialized="view"
  )
}}

{% set fieldPrefix = 'apcomsup_' %}

select
    {{ dbt_utils.star(ref('appuiscommuns_supportaerien__example'),
        except=[fieldPrefix + 'Id', 'geometry', fieldPrefix + 'HauteurAppui']) }},
    "{{ fieldPrefix }}Id"::uuid,
    "{{ fieldPrefix }}HauteurAppui"::numeric,
    ST_GeomFROMText(geometry, 4326) as geometry -- NOT ::geometry else not the same (srid ?? only visible in binary ::text form : ) therefore except does not work
    -- 0101000000197B8A77DBE0E33F18C25725ECC34740 expected
    -- 0101000020E6100000197B8A77DBE0E33F18C25725ECC34740 actual
    -- TODO rm :
    ----'"datastore"."appuiscommuns"."osmgeodatamine_powsupp__appuiscommuns_supportaerien"' as _dbt_source_relation,
    --appuiscommunssupp__fdrcommune__insee_id as appuiscommunssupp__commune_insee_id,
    ----appuiscommunssupp__fdrcommune__insee_id as fdrcommune__insee_id
    --appuiscommunssupp__fdrcommune__nom as appuiscommunssupp__commune_nom
    
    from {{ ref('appuiscommuns_supportaerien__example') }} -- TODO raw_