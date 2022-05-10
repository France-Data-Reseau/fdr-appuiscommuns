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

{% macro apcom_supportaerien_translation__link_geometry_fdrcommune(translated_source, id_field, fields, order_by=None) %}

{% set field_min_cast_types = { "geometry" : "geometry" } %}

{% set fieldPrefix = 'apcomsup_' %} -- for debug purpose only

-- apcom_supportaerien_translation__link_geometry_fdrcommune :

with link_candidates as (
    -- 5s on 1m lines
    select
        {% for field in fields | reject("eq", fieldPrefix + "com_code") | reject("eq", fieldPrefix + "com_code__ar") | reject("eq", fieldPrefix + "com_code__arr_len") %}
          {{ translated_source }}.{{ adapter.quote(field) }},
        {% endfor %}
        --{{ translated_source }}."{{ fieldPrefix }}Id", -- * not possible for group by below
        -- other fields useful to not have to join to commune :
        -- (reusing official INSEE field names, since well known)
        c.com_code as {{ fieldPrefix }}com_code, --as "apcomsup_fdrcom_insee_id", -- the actual FK field
        -- field names :
        -- non-semantized names will conflict on join unless specifying relation alias,
        -- but semantized names require having a relation _definition.sql
        c.com_code, --as "fdrcom_insee_id",
        c.com_name, --as "fdrcom_nom",
        c.epci_code, --as "fdrepci_insee_id", -- not used, keep ?
        c.epci_name, --as "fdrepci_nom",
        c.dep_code, --as "fdrdep_insee_id", -- not used, keep ?
        c.dep_name, --as "fdrdep_nom",
        c.reg_code, --as "fdrreg_insee_id",
        c.reg_name --as "fdrreg_nom"
    --FROM computed, {{ source('france-data-reseau', 'georef-france-commune.csv') }} c
    FROM {{ translated_source }}, {{ ref('georef-france-commune.csv') }} c
    --WHERE ST_Contains(ST_GeometryFromText(ST_AsText(c.geo_shape), 4326), {{ translated_source }}.geometry) and c.com_code is not null -- TODO patch source geometry to 4326 SRID
    WHERE ST_Contains(c.geo_shape_4326, {{ translated_source }}.geometry) and c.com_code is not null -- ! removes communes of Nouvelle Calédonie etc.

), link_candidate_array as (
    -- no performance change, else 2 array_agg would gave to be inlined ;
    -- TODO and "updated" or row_count() for ORDER BY LIMIT 1 ? LATER macro & FILTER NOT NULL http///
    select
        link_candidates."{{ id_field }}", -- !!! uuid does not support min() ; or (ARRAY_AGG("{{ id_field }}") FILTER (WHERE "{{ id_field }}" IS NOT NULL))[1] as "{{ id_field }}",
        {% for field in fields | reject("eq", id_field) | reject("eq", fieldPrefix + "com_code") | reject("eq", fieldPrefix + "com_code__ar") | reject("eq", fieldPrefix + "com_code__arr_len") %}
          -- min requires re cast in case of ex. geometry :
          --min(link_candidates.{{ adapter.quote(field) }}){{ "::" ~ field_min_cast_types.get(field) if field_min_cast_types.get(field) else "" }} as {{ adapter.quote(field) }},
          -- ARRAY_AGG is simpler and maybe as fast but might take any value whatever the order (which is not a problem here) :
          (ARRAY_AGG(link_candidates.{{ adapter.quote(field) }}) FILTER (WHERE link_candidates.{{ adapter.quote(field) }} IS NOT NULL))[1] as {{ adapter.quote(field) }},
        {% endfor %}
        --link_candidates.*,--"{{ fieldPrefix }}Id",
        --(ARRAY_AGG("fdrcommune__insee_id") FILTER (WHERE "fdrcommune__insee_id" IS NOT NULL order by "updated" desc limit 1))[1] as "fdrcommune__insee_id",
        -- see : https://stackoverflow.com/questions/61874745/postgresql-get-first-non-null-value-per-group
        -- https://github.com/dbt-labs/dbt-utils/issues/335 https://github.com/dbt-labs/dbt-utils/pull/29
        {# semantized names version
        ARRAY_AGG(link_candidates."apcomsup_fdrcom_insee_id" ORDER BY link_candidates."fdrcom_insee_id") as "apcomsup_fdrcom_insee_id__arr",
        ARRAY_AGG(link_candidates."fdrcom_insee_id" ORDER BY link_candidates."fdrcom_insee_id") as "fdrcom_insee_id__arr",
        ARRAY_AGG(link_candidates."fdrcom_nom" ORDER BY link_candidates."fdrcom_insee_id") as "fdrcom_nom__arr",
        ARRAY_AGG(link_candidates."fdrepci_insee_id" ORDER BY link_candidates."fdrcom_insee_id") as "fdrepci_insee_id__arr",
        ARRAY_AGG(link_candidates."fdrepci_nom" ORDER BY link_candidates."fdrcom_insee_id") as "fdrepci_nom__arr",
        ARRAY_AGG(link_candidates."fdrdep_insee_id" ORDER BY link_candidates."fdrcom_insee_id") as "fdrdep_insee_id__arr",
        ARRAY_AGG(link_candidates."fdrdep_nom" ORDER BY link_candidates."fdrcom_insee_id") as "fdrdep_nom__arr",
        ARRAY_AGG(link_candidates."fdrreg_insee_id" ORDER BY link_candidates."fdrcom_insee_id") as "fdrre_insee_id__arr",
        ARRAY_AGG(link_candidates."fdrreg_nom" ORDER BY link_candidates."fdrcom_insee_id") as "fdrre_nom__arr",
        #}
        ARRAY_AGG(link_candidates."apcomsup_com_code" ORDER BY link_candidates."com_code") as "apcomsup_com_code__arr",
        ARRAY_AGG(link_candidates."com_code" ORDER BY link_candidates."com_code") as "com_code__arr",
        ARRAY_AGG(link_candidates."com_name" ORDER BY link_candidates."com_code") as "com_name__arr",
        ARRAY_AGG(link_candidates."epci_code" ORDER BY link_candidates."com_code") as "epci_code__arr",
        ARRAY_AGG(link_candidates."epci_name" ORDER BY link_candidates."com_code") as "epci_name__arr",
        ARRAY_AGG(link_candidates."dep_code" ORDER BY link_candidates."com_code") as "dep_code__arr",
        ARRAY_AGG(link_candidates."dep_name" ORDER BY link_candidates."com_code") as "dep_name__arr",
        ARRAY_AGG(link_candidates."reg_code" ORDER BY link_candidates."com_code") as "reg_code__arr",
        ARRAY_AGG(link_candidates."reg_name" ORDER BY link_candidates."com_code") as "reg_name__arr",
        count(link_candidates.*) as "com_code__arr_len"
    from link_candidates
    group by "{{ id_field }}"
    {% if order_by %})
      order by {{ order_by }} -- "{{ fieldPrefix }}Id", "fdrcommune__insee_id"
    {% endif %}

), link_candidate as (
    -- no performance change
    select
        *,--"{{ fieldPrefix }}Id",
        {# semantized names version
        ("apcomsup_fdrcom_insee_id__arr")[1] as "apcomsup_fdrcom_insee_id",
        ("fdrcommune_insee_id__arr")[1] as "fdrcommune_insee_id",
        ("fdrcommune_nom__arr")[1] as "fdrcommune_nom",
        ("fdrepci_insee_id__arr")[1] as "fdrepci_insee_id",
        ("fdrepci_nom__arr")[1] as "fdrepci_nom",
        ("fdrdep_insee_id__arr")[1] as "fdrdep_insee_id",
        ("fdrdep_nom__arr")[1] as "fdrdep_nom",
        ("fdrregion_insee_id__arr")[1] as "fdrregion_insee_id",
        ("fdrregion_nom__arr")[1] as "fdrregion_nom"
        --case ("fdrcommune__insee_id__arr")[1] when null then fdrcommune__insee_id else ("fdrcommune__insee_id__arr")[1] as "fdrcommune__insee_id",
        ----"fdrcommune__insee_id__arr",
        ----"fdrcommune__insee_id__arr_len"
        #}
        ("apcomsup_com_code__arr")[1] as "apcomsup_com_code",
        ("com_code__arr")[1] as "com_code",
        ("com_name__arr")[1] as "com_name",
        ("epci_code__arr")[1] as "epci_code",
        ("epci_name__arr")[1] as "epci_name",
        ("dep_code__arr")[1] as "dep_code",
        ("dep_name__arr")[1] as "dep_name",
        ("reg_code__arr")[1] as "reg_code",
        ("reg_name__arr")[1] as "reg_name"
    from link_candidate_array

)

select * from link_candidate
          
{% endmacro %}