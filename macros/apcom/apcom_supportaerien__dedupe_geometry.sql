{#
2 phase dedup : first create matching pairs, then merge them (here using a merge resolution order)
Both steps could be separated, and the merge be done in another, usage-specific macro.

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
#}

-- TODO apcom_supportaerien_translation__dup_geometry : first step producing only duplicates,
-- that can be merged according to the expert choices afterwards (rather than static rules)
{% macro apcom_supportaerien_translation__dedupe_geometry(normalized_source_model_name, id_field, order_by_fields, fields, criteria) %}

{% set fieldPrefix = 'apcomsup_' %} -- for debug purpose only

{% set id_fields = [id_field] %}
{# % set order_by_fields = [src_name_field, src_id_field] % #}
{% set id_and_order_by_fields = id_fields + order_by_fields %}

{% set pair_order_fields = [] %}

-- apcom_supportaerien_translation__dup_geometry :

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

        , earlier.geometry as earlier_geometry, later.geometry as later_geometry -- for debugging purpose
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
    --ST_Distance(ST_Transform(earlier.geometry, 3857), ST_Transform(later.geometry, 3857)) < {{ distance_m }} -- requires transform because 4326 distance is in degrees ; assuming geometry's not NULL
    -- within box is more efficient https://postgis.net/workshops/postgis-intro/knn.html :
    --and ST_Expand(earlier.geometry, {{ distance_m }}) && later.geometry -- TODO REQUIRES CONVERT and then ST_Within faster
    -- see https://gis.stackexchange.com/questions/93936/searching-planet-osm-point-by-longitude-and-latitude/93957#93957 https://gis.stackexchange.com/questions/94886/st-expand-return-different-results-depending-on-meters
    --and ST_DWithin(way, ST_Transform(later.geometry, 3857),
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

), filtered as (

  -- 2. remove lines where later / right part has already been mentioned before as earlier / left part
  -- i.e. previous groups will already have included them :
  select * from link_candidates current_lc
  where "later{{ id_field }}" not in (
    select "later{{ id_field }}" from link_candidates lc_before_current_one
    WHERE
    {% for i in range(0, pair_order_fields | length) %}
      (
        {% for field in pair_order_fields[:i+1] %}
          lc_before_current_one."{{ field }}" {% if not loop.last %}={% else %}<{% endif %} current_lc."{{ field }}" {% if not loop.last %}and{% endif %}
        {% endfor %}
      )
      {% if not loop.last %}or{% endif %}
    {% endfor %}
    ---- lc_before_current_one."earlier{{ src_name_field }}" < current_lc."earlier{{ src_name_field }}" -- only between different sources
    ---- or (lc_before_current_one."earlier{{ src_name_field }}" = current_lc."earlier{{ src_name_field }}"
    ----   and lc_before_current_one."earlier{{ src_id_field }}" < current_lc."earlier{{ src_id_field }}") -- only between different sources
    ---- or (lc_before_current_one."earlier{{ src_name_field }}" = current_lc."earlier{{ src_name_field }}"
    ----   and lc_before_current_one."earlier{{ src_id_field }}" = current_lc."earlier{{ src_id_field }}" -- only between different sources
    ----   and lc_before_current_one."later{{ src_name_field }}" < current_lc."later{{ src_name_field }}")
    ---- or (lc_before_current_one."earlier{{ src_name_field }}" = current_lc."earlier{{ src_name_field }}"
    ----   and lc_before_current_one."earlier{{ src_id_field }}" = current_lc."earlier{{ src_id_field }}" -- only between different sources
    ----   and lc_before_current_one."later{{ src_name_field }}" = current_lc."later{{ src_name_field }}"
    ----   and lc_before_current_one."later{{ src_id_field }}" < current_lc."later{{ src_id_field }}")
    --and earlier."{{ src_id_field }}" < later."{{ src_id_field }}" -- ONLY IF WERE IN SAME SOURCE assuming _src_id is the model's ordering field
  )

), filtered_plus as (
  select * from (
  select
        {% for field in id_and_order_by_fields %}
          "earlier{{ field }}",
        {% endfor %}
        {% for field in id_and_order_by_fields %}
          "earlier{{ field }}" as "later{{ field }}" {% if not loop.last%},{% endif %}
        {% endfor %}
    ---- "earlier{{ src_name_field }}", "earlier{{ src_id_field }}", "earlier{{ id_field }}",
    ---- "earlier{{ src_name_field }}" as "later{{ src_name_field }}", "earlier{{ src_id_field }}" as "later{{ src_id_field }}",  "earlier{{ id_field }}" as "later{{ id_field }}"

    , earlier_geometry as earlier_geometry, earlier_geometry as later_geometry -- for debugging purpose
    from filtered
  union
  select * from filtered
  ) fp
  order by
    {% for order_by_field in order_by_fields %}
      "earlier{{ order_by_field }}" asc,
    {% endfor %}
    {% for order_by_field in order_by_fields %}
      "later{{ order_by_field }}" asc {% if not loop.last%},{% endif %}
    {% endfor %}
  ---- "earlier{{ src_name_field }}" asc, "earlier{{ src_id_field }}" asc, "later{{ src_name_field }}" asc, "later{{ src_id_field }}" asc

  -- "{{ fieldPrefix }}src_name", "{{ fieldPrefix }}src_id"
  --{% for order_by_field in order_by_fields %}
  --filtered_plus.{{ adapter.quote(order_by_field) }} {% if not loop.last %},{% endif %}
  --{% endfor %}

), to_be_merged_ids as (

  select
    distinct "earlier{{ id_field }}"
  from filtered
  union
  select
    distinct "later{{ id_field }}"
  from filtered
  -- (and once filtered no later_id is among earlier_id)

), merged as (

   {# order of merge (only) of values (within already chosen merged_ids) : #}
   {% set earlier_order_by = '"earlier' + '" asc, "earlier'.join(order_by_fields) + '" asc' %} -- "earlier{{ src_name_field }}" asc, "earlier{{ src_id_field }}" asc

  -- TODO and "updated" or row_count() for ORDER BY LIMIT 1 ? LATER macro & FILTER NOT NULL http///
  select
      filtered_plus."earlier{{ id_field }}" as "{{ id_field }}", -- !!! uuid does not support min() ; or (ARRAY_AGG("{{ id_field }}") FILTER (WHERE "{{ id_field }}" IS NOT NULL))[1] as "{{ id_field }}",
      {% for field in fields | reject("eq", id_field) %}
        -- get not null appearing first according to src_name (ex. OSM last) and bigger src_id (most recent first) :
        -- order see https://stackoverflow.com/questions/7317475/postgresql-array-agg-order
        (ARRAY_AGG(all1.{{ adapter.quote(field) }} ORDER BY {{ earlier_order_by }}) FILTER (WHERE all1.{{ adapter.quote(field) }} IS NOT NULL))[1] as {{ adapter.quote(field) }},
      {% endfor %}
      --filtered_plus.*,--"{{ id_field }}",
      -- debug :
      --(ARRAY_AGG("fdrcommune__insee_id") FILTER (WHERE "fdrcommune__insee_id" IS NOT NULL order by "updated" asc limit 1))[1] as "fdrcommune__insee_id",
      -- see : https://stackoverflow.com/questions/61874745/postgresql-get-first-non-null-value-per-group
      -- https://github.com/dbt-labs/dbt-utils/issues/335 https://github.com/dbt-labs/dbt-utils/pull/29
      (ARRAY_AGG(filtered_plus."later{{ id_field }}" ORDER BY {{ earlier_order_by }}) FILTER (WHERE filtered_plus."later{{ id_field }}" <> filtered_plus."earlier{{ id_field }}")) as "merged_ids",
      (count(filtered_plus."later{{ id_field }}") - 1) as "merged_ids_nb"
  from filtered_plus
  join {{ ref(normalized_source_model_name) }} all1 on filtered_plus."later{{ id_field }}" = all1."{{ id_field }}"
  group by filtered_plus."earlier{{ id_field }}"
  -- NO ORDER BY ELSE error column "filtered_plus.earlier{{ src_name_field }}" must appear in the GROUP BY clause or be used in an aggregate function ; order of preference : by default of source then of its order field (unless all sources are dated)

), all_merged as (

  select
      "{{ id_field }}",
      {% for field in fields | reject("eq", id_field) %}
         {{ adapter.quote(field) }},
      {% endfor %}
      NULL as merged_ids,
      NULL as merged_ids_nb
  from {{ ref(normalized_source_model_name) }}
  where "{{ id_field }}" not in (select * from to_be_merged_ids)
  union
  select * from merged

)

select * from all_merged
  order by
    ---- all_merged."{{ src_name_field }}" asc, all_merged."{{ src_id_field }}" asc
    {% for order_by_field in order_by_fields %}
      "{{ order_by_field }}" asc {% if not loop.last%},{% endif %}
    {% endfor %}
  --{% for order_by_field in order_by_fields %}
  --all_merged.{{ adapter.quote(order_by_field) }} {% if not loop.last %},{% endif %}
  --{% endfor %}

{% endmacro %}