{#
Definition / interface
- with the proper column types (thanks to _example_stg),
- but without any data (to allow to use to define columns in sql ex. as first in union)

Materialized as table because of these uses.
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

{{
  config(
    materialized="table"
  )
}}

select
    {{ dbt_utils.star(ref('appuiscommuns_supportaerien__example_stg')) }}
    
    from {{ ref('appuiscommuns_supportaerien__example_stg') }} -- TODO raw_
    limit 0