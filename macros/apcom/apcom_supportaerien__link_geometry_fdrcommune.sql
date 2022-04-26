{#
1 phase n-n reconciliation / linking - produce array of linked object ids from subject
Partie générique - link_geometry_fdrcommune, utilisé dans apcom_supportaerien

TODO quite generic macro, still more

parameters :
- translated_source : SQL alias or relation
- id_field
- fields : required for the min() step
- field_min_cast_types : type of fields that need to be cast back from text after min() step outside id_field
#}

{% macro apcom_supportaerien_translation__link_geometry_fdrcommune(translated_source, id_field, fields, field_min_cast_types, order_by=None) %}

{% set field_min_cast_types = { "geometry" : "geometry" } %}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien_osmgeodatamine_powersupports_extract' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'osmpowersupports' %} -- ?
{% set prefix = 'appuiscommunssupp' %} -- ?+
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '__' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

with link_candidates as (
    -- 5s on 1m lines
    select
        {% for field in fields | reject("eq", "fdrcommune__insee_id") %}
          {{ translated_source }}.{{ adapter.quote(field) }},
        {% endfor %}
        --{{ translated_source }}."{{ fieldPrefix }}Id", -- * not possible for group by below
        c.com_code as "fdrcommune__insee_id"
    --FROM computed, {{ source('france-data-reseau', 'georef-france-commune.csv') }} c
    FROM {{ translated_source }}, {{ ref('georef-france-commune.csv') }} c
    --WHERE ST_Contains(ST_GeometryFromText(ST_AsText(c.geo_shape), 4326), {{ translated_source }}.geometry) and c.com_code is not null -- TODO patch source geometry to 4326 SRID
    WHERE ST_Contains(c.geo_shape_4326, {{ translated_source }}.geometry) and c.com_code is not null -- ! removes communes of Nouvelle Calédonie etc.

), link_candidate_array as (
    -- no performance change, else 2 array_agg would gave to be inlined ;
    -- TODO and "updated" or row_count() for ORDER BY LIMIT 1 ? LATER macro & FILTER NOT NULL http///
    select
        link_candidates."{{ id_field }}", -- !!! uuid does not support min() ; or (ARRAY_AGG("{{ id_field }}") FILTER (WHERE "{{ id_field }}" IS NOT NULL))[1] as "{{ id_field }}",
        {% for field in fields | reject("eq", "fdrcommune__insee_id") | reject("eq", id_field) %}
          min(link_candidates.{{ adapter.quote(field) }}){{ "::" ~ field_min_cast_types.get(field) if field_min_cast_types.get(field) else "" }} as {{ adapter.quote(field) }},
        {% endfor %}
        --link_candidates.*,--"{{ fieldPrefix }}Id",
        --(ARRAY_AGG("fdrcommune__insee_id") FILTER (WHERE "fdrcommune__insee_id" IS NOT NULL order by "updated" desc limit 1))[1] as "fdrcommune__insee_id",
        -- see : https://stackoverflow.com/questions/61874745/postgresql-get-first-non-null-value-per-group
        -- https://github.com/dbt-labs/dbt-utils/issues/335 https://github.com/dbt-labs/dbt-utils/pull/29
        ARRAY_AGG(link_candidates."fdrcommune__insee_id") as "fdrcommune__insee_id__arr",
        count(link_candidates.*) as "fdrcommune__insee_id__arr_len"
    from link_candidates
    group by "{{ id_field }}"
    {% if order_by %})
      order by {{ order_by }} -- "{{ fieldPrefix }}Id", "fdrcommune__insee_id"
    {% endif %}
    
), link_candidate as (
    -- no performance change
    select
        link_candidate_array.*,--"{{ fieldPrefix }}Id",
        ("fdrcommune__insee_id__arr")[1] as "fdrcommune__insee_id"--,
        --case ("fdrcommune__insee_id__arr")[1] when null then fdrcommune__insee_id else ("fdrcommune__insee_id__arr")[1] as "fdrcommune__insee_id",
        ----"fdrcommune__insee_id__arr",
        ----"fdrcommune__insee_id__arr_len"
    from link_candidate_array

)

select * from link_candidate
          
{% endmacro %}