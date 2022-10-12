{#
2 phase deduplication : phase 1 - first creates matching pairs (according to provided SQL criteria),
supports DBT incremental (filtered on later.last_changed whose max is added as a dummy marker line, use it with
unique_key=['"earlier' + fieldPrefix + "IdSupportAerien" + '"', '"later' + fieldPrefix + "IdSupportAerien" + '"'])

TODO obviously faster if indexed on id fields (source name and id : _src_name, _src_id)
TODO and additional order fields

TODO example

parameters :
- normalized_source_model_name
- id_field : used as the id of a merge line, by which the merging "group by" is done.
TODO id_field. Can be order_by_fields if they are unique.
- order_by_fields : the ASC ordering, to ensure not to have duplicate matches,
then of merge resolution
- fields : to merge across matched duplicates and include in the final product
- criteria : SQL criteria that defines the duplicate matches
- TODO debug fields (geometry...) : to pass along

returns merged lines of data with :
- all input fields. The first non-NULL value according to the merge order (order_by_fields)
- merged_ids : ARRAY of all ids merged in a single line of data
- ordered by order_by_fields
#}

-- TODO apcom_supportaerien_translation__dup_geometry : first step producing only duplicates,
-- that can be merged according to the expert choices afterwards (rather than static rules)
{% macro apcom_supportaerien_dedupe_geometry_candidates(normalized_source_model_name,
    id_field, order_by_fields, geometry_field, criteria) %}

{% set fieldPrefix = 'apcomsup_' %} -- for debug purpose only

{% set id_fields = [id_field] %}
{# % set order_by_fields = [src_name_field, src_id_field] % #}
{% set id_and_order_by_fields = id_fields + order_by_fields %}

{% set pair_order_fields = [] %}

with link_candidates as (

    -- 1. Let's match data with itself and build the pairs where the criteria is met,
    -- but list each pair only once / on one side by using an ordering :
    -- xs on 1m lines
    select
        {% for field in id_and_order_by_fields %}
          earlier."{{ field }}" as "earlier{{ field }}",
        {% endfor %}
        {% for field in id_and_order_by_fields %}
          later."{{ field }}" as "later{{ field }}" {% if not loop.last%},{% endif %}
        {% endfor %}
        ---- earlier."{{ src_name_field }}" as "earlier{{ src_name_field }}",
        ---- earlier."{{ src_id_field }}" as "earlier{{ src_id_field }}", -- to be able to order before group by (TODO Q but does it use it ?)
        ---- earlier."{{ id_field }}" as "earlier{{ id_field }}",
        ---- later."{{ src_name_field }}" as "later{{ src_name_field }}",
        ---- later."{{ src_id_field }}" as "later{{ src_id_field }}",
        ---- later."{{ id_field }}" as "later{{ id_field }}"

        , later.last_changed as later_last_changed

        , earlier."{{ geometry_field }}" as earlier_geometry, later."{{ geometry_field }}" as later_geometry -- for debugging purpose
    FROM {{ ref(normalized_source_model_name) }} earlier, {{ ref(normalized_source_model_name) }} later
    WHERE
    -- preference of data among lines (which is earliest in "earlier" / left side), according to order_by :
    -- only list pairs whose right / later side is after its left / earlier side
    -- (in this ordering, the first left / earlier one may match all right / later ones,
    -- then the second one may match all right / later ones except the first one (so all next ones)...)
    (
    {% for i in range(0, order_by_fields | length) %}
      (
        {% for field in order_by_fields[:i+1] %}
          earlier."{{ field }}" {% if not loop.last %}={% else %}<{% endif %} later."{{ field }}" {% if not loop.last %}and{% endif %}
        {% endfor %}
      )
      {% if not loop.last %}or{% endif %}
    {% endfor %}
    )
    ----earlier."{{ src_name_field }}" < later."{{ src_name_field }}" -- only between different sources
    --and earlier."{{ src_id_field }}" < later."{{ src_id_field }}" -- ONLY IF WERE IN SAME SOURCE assuming _src_id is the model's ordering field
    and {{ criteria }}
    --ST_Distance(ST_Transform(earlier."{{ geometry_field }}", 3857), ST_Transform(later."{{ geometry_field }}", 3857)) < {{ distance_m }} -- requires transform because 4326 distance is in degrees ; assuming geometry's not NULL
    -- within box is more efficient https://postgis.net/workshops/postgis-intro/knn.html :
    --and ST_Expand(earlier."{{ geometry_field }}", {{ distance_m }}) && later."{{ geometry_field }}" -- TODO REQUIRES CONVERT and then ST_Within faster
    -- see https://gis.stackexchange.com/questions/93936/searching-planet-osm-point-by-longitude-and-latitude/93957#93957 https://gis.stackexchange.com/questions/94886/st-expand-return-different-results-depending-on-meters
    --and ST_DWithin(way, ST_Transform(later."{{ geometry_field }}", 3857),

    --and 1 = (1+1) -- test incremental without table already existing
    {% if is_incremental() %}
      --and later.last_changed <= '2022-09-30T15:30:28' -- test incremental in the middle, or change the later_last_changed column
      and later.last_changed > (select coalesce(max(max_sup.later_last_changed), '1970-01-01T00:00:00') from {{ this }} max_sup where max_sup."earlier_geometry" is NULL)
    {% endif %}

    ORDER BY
    {% for order_by_field in order_by_fields %}
      "earlier{{ order_by_field }}" asc,
      -- {{ pair_order_fields.append("earlier" ~ order_by_field) }}
    {% endfor %}
    {% for order_by_field in order_by_fields %}
      "later{{ order_by_field }}" asc {% if not loop.last%},{% endif %}
      -- {{ pair_order_fields.append("later" ~ order_by_field) }}
    {% endfor %}
    ---- earlier."{{ src_name_field }}" asc, earlier."{{ src_id_field }}" asc,
    ---- later."{{ src_name_field }}" asc, later."{{ src_id_field }}" asc
)
select * from link_candidates

-- adding last_changed of processed data lines :
UNION ALL
(
    select
        {% for field in id_and_order_by_fields %}
          'INCREMENTAL_MAX_DUMMY' as "earlier{{ field }}",
        {% endfor %}
        {% for field in id_and_order_by_fields %}
          'INCREMENTAL_MAX_DUMMY' as "later{{ field }}",
        {% endfor %}
        ---- earlier."{{ src_name_field }}" as "earlier{{ src_name_field }}",
        ---- earlier."{{ src_id_field }}" as "earlier{{ src_id_field }}", -- to be able to order before group by (TODO Q but does it use it ?)
        ---- earlier."{{ id_field }}" as "earlier{{ id_field }}",
        ---- later."{{ src_name_field }}" as "later{{ src_name_field }}",
        ---- later."{{ src_id_field }}" as "later{{ src_id_field }}",
        ---- later."{{ id_field }}" as "later{{ id_field }}"

        (select max(last_changed) from {{ ref(normalized_source_model_name) }}) as later_last_changed

        , NULL as earlier_geometry, NULL as later_geometry -- for debugging purpose
)

{% endmacro %}