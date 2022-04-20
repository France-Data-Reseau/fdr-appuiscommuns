{#
Parsing de l'attendu _expected du test unitaire de normalization
#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien_osmgeodatamine_powersupports_extract' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'osmpowersupports' %} -- ?
{% set prefix = 'appuiscommunssupp' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '__' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{# __expected and NOT __definition else also asks for the fields not provided by this source #}
select
    {{ dbt_utils.star(ref('osm_powersupports__supportaerien__expected'),
        except=[fieldPrefix + 'Id', 'geometry']) }},
    "{{ fieldPrefix }}Id"::uuid as "{{ fieldPrefix }}Id",
    ST_GeomFROMText(geometry, 4326) as geometry -- NOT ::geometry else not the same (srid ?? only visible in binary ::text form : ) therefore except does not work
    -- 0101000000197B8A77DBE0E33F18C25725ECC34740 expected
    -- 0101000020E6100000197B8A77DBE0E33F18C25725ECC34740 actual
    -- TODO rm :
    ----'"datastore"."appuiscommuns"."osmgeodatamine_powsupp__appuiscommuns_supportaerien"' as _dbt_source_relation,
    --appuiscommunssupp__fdrcommune__insee_id as appuiscommunssupp__commune_insee_id,
    ----appuiscommunssupp__fdrcommune__insee_id as fdrcommune__insee_id
    --appuiscommunssupp__fdrcommune__nom as appuiscommunssupp__commune_nom
    
    from {{ ref('osm_powersupports__supportaerien__expected') }} -- TODO raw_