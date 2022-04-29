{#
2 phase dedup - phase 1

TODO obviously faster if indexed on id fields (source name and id : _src_name, _src_id)
TODO and additional order fields

TODO example
#}

{% macro apcom_supportaerien_translation__dup_geometry(normalized_source_model_name, id_field, src_name_field, src_id_field, fields, criteria, order_by_fields2) %}

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

{% set id_fields = [id_field] %}
{% set order_by_fields = [src_name_field, src_id_field] %}
{% set id_and_order_by_fields = id_fields + order_by_fields %}

with link_candidates as (
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
    WHERE earlier."{{ src_name_field }}" < later."{{ src_name_field }}" -- only between different sources
    --and earlier."{{ src_id_field }}" < later."{{ src_id_field }}" -- ONLY IF WERE IN SAME SOURCE assuming _src_id is the model's ordering field
    and {{ criteria }}
    --ST_Distance(ST_Transform(earlier.geometry, 3857), ST_Transform(later.geometry, 3857)) < {{ distance_m }} -- requires transform because 4326 distance is in degrees ; assuming geometry's not NULL
    -- within box is more efficient https://postgis.net/workshops/postgis-intro/knn.html :
    --and ST_Expand(earlier.geometry, {{ distance_m }}) && later.geometry -- TODO REQUIRES CONVERT and then ST_Within faster
    -- see https://gis.stackexchange.com/questions/93936/searching-planet-osm-point-by-longitude-and-latitude/93957#93957 https://gis.stackexchange.com/questions/94886/st-expand-return-different-results-depending-on-meters
    --and ST_DWithin(way, ST_Transform(later.geometry, 3857),
    ORDER BY
    {% for order_by_field in order_by_fields %}
      "earlier{{ order_by_field }}" desc,
    {% endfor %}
    {% for order_by_field in order_by_fields %}
      "later{{ order_by_field }}" desc {% if not loop.last%},{% endif %}
    {% endfor %}
    ---- earlier."{{ src_name_field }}" asc, earlier."{{ src_id_field }}" desc,
    ---- later."{{ src_name_field }}" asc, later."{{ src_id_field }}" desc

), filtered as (

  -- remove lines where later / right part has already been mentioned before as earlier / left part
  -- i.e. previous groups will already have included them :
  select * from link_candidates current_lc
  where "later{{ id_field }}" not in (
    select "later{{ id_field }}" from link_candidates lc_before_current_one
    WHERE
    lc_before_current_one."earlier{{ src_name_field }}" < current_lc."earlier{{ src_name_field }}" -- only between different sources
    or (lc_before_current_one."earlier{{ src_name_field }}" = current_lc."earlier{{ src_name_field }}"
      and lc_before_current_one."earlier{{ src_id_field }}" < current_lc."earlier{{ src_id_field }}") -- only between different sources
    or (lc_before_current_one."earlier{{ src_name_field }}" = current_lc."earlier{{ src_name_field }}"
      and lc_before_current_one."earlier{{ src_id_field }}" = current_lc."earlier{{ src_id_field }}" -- only between different sources
      and lc_before_current_one."later{{ src_name_field }}" < current_lc."later{{ src_name_field }}")
    or (lc_before_current_one."earlier{{ src_name_field }}" = current_lc."earlier{{ src_name_field }}"
      and lc_before_current_one."earlier{{ src_id_field }}" = current_lc."earlier{{ src_id_field }}" -- only between different sources
      and lc_before_current_one."later{{ src_name_field }}" = current_lc."later{{ src_name_field }}"
      and lc_before_current_one."later{{ src_id_field }}" < current_lc."later{{ src_id_field }}")
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
  -- order of preference of data among lines :
  order by
    {% for order_by_field in order_by_fields %}
      "earlier{{ order_by_field }}" desc,
    {% endfor %}
    {% for order_by_field in order_by_fields %}
      "later{{ order_by_field }}" desc {% if not loop.last%},{% endif %}
    {% endfor %}
  ---- "earlier{{ src_name_field }}" asc, "earlier{{ src_id_field }}" desc, "later{{ src_name_field }}" ASC, "later{{ src_id_field }}" desc

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

  -- TODO and "updated" or row_count() for ORDER BY LIMIT 1 ? LATER macro & FILTER NOT NULL http///
  select
      filtered_plus."earlier{{ id_field }}" as "{{ id_field }}", -- !!! uuid does not support min() ; or (ARRAY_AGG("{{ id_field }}") FILTER (WHERE "{{ id_field }}" IS NOT NULL))[1] as "{{ id_field }}",
      {% for field in fields | reject("eq", id_field) %}
        -- get not null appearing first according to src_name (ex. OSM last) and bigger src_id (most recent first) :
        -- order see https://stackoverflow.com/questions/7317475/postgresql-array-agg-order
        (ARRAY_AGG(all1.{{ adapter.quote(field) }} ORDER BY earlier{{ src_name_field }} ASC, "earlier{{ src_id_field }}" DESC) FILTER (WHERE all1.{{ adapter.quote(field) }} IS NOT NULL))[1] as {{ adapter.quote(field) }},
      {% endfor %}
      --filtered_plus.*,--"{{ id_field }}",
      -- debug :
      --(ARRAY_AGG("fdrcommune__insee_id") FILTER (WHERE "fdrcommune__insee_id" IS NOT NULL order by "updated" desc limit 1))[1] as "fdrcommune__insee_id",
      -- see : https://stackoverflow.com/questions/61874745/postgresql-get-first-non-null-value-per-group
      -- https://github.com/dbt-labs/dbt-utils/issues/335 https://github.com/dbt-labs/dbt-utils/pull/29
      (ARRAY_AGG(filtered_plus."later{{ id_field }}" ORDER BY "earlier{{ src_name_field }}" ASC, "earlier{{ src_id_field }}" DESC) FILTER (WHERE filtered_plus."later{{ id_field }}" <> filtered_plus."earlier{{ id_field }}")) as "merged_ids",
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
    ---- all_merged."{{ src_name_field }}" asc, all_merged."{{ src_id_field }}" desc
    {% for order_by_field in order_by_fields %}
      all_merged."{{ order_by_field }}" desc {% if not loop.last%},{% endif %}
    {% endfor %}
  --{% for order_by_field in order_by_fields %}
  --all_merged.{{ adapter.quote(order_by_field) }} {% if not loop.last %},{% endif %}
  --{% endfor %}
  
{% endmacro %}