{#
2 phase n-n reconciliation / linking - phase 1 produce linked object ids from subject
#}

{% macro apcom_supportaerien__2phase1link_fdrcommune_geometry(translated_source, id_field, fields, order_by=None) %}

{% set field_min_cast_types = { "geometry" : "geometry" } %}

{% set fieldPrefix = 'apcomsup_' %} -- for debug purpose only

-- apcom_supportaerien__2phase1link_fdrcommune_geometry :

with link_candidates as (
    -- 54s on 1m osmposup
    select
        {{ translated_source }}."{{ id_field }}", -- * not possible for group by below
        com.com_code,-- as "fdrcommune__insee_id",
        -- other small useful fields to avoid joining to commune most of the times :
        com.com_name,-- as "fdrcommune__name",
        com.reg_code,-- as "fdregion__insee_id",
        com.reg_name -- as "fdregion__name"
    FROM {{ translated_source }}, {{ source('france-data-reseau', 'georef-france-commune_old.csv') }} com
    {# FROM {{ translated_source }}, {{ ref('georef-france-commune.csv') }} com #}
    --WHERE ST_Contains(ST_GeometryFromText(ST_AsText(c.geo_shape), 4326), {{ translated_source }}.geometry) and c.com_code is not null -- TODO patch source geometry to 4326 SRID
    WHERE ST_Contains(com.geo_shape_4326, {{ translated_source }}.geometry) and com.com_code is not null -- ! removes communes of Nouvelle Calédonie etc.
    --having count(*) > 1 -- TODO idea : store only rare duplicates
    {% if order_by %})
      order by {{ order_by }} -- "{{ fieldPrefix }}Id", "fdrcommune__insee_id"
    {% endif %}
)

select * from link_candidates
          
{% endmacro %}