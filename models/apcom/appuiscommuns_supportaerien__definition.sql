{#
Definition / interface
- with the proper column types (thanks to _example_stg),
- but without any data (to allow to use to define columns in sql ex. as first in union)

Materialized as view because of these uses.

    alias='wrong'
#}

{{
  config(
    materialized="view"
  )
}}

{% set source_model = ref('appuiscommuns_supportaerien__example_stg') %}

select
    {{ dbt_utils.star(source_model) }}
    
    from {{ source_model }} -- TODO raw_
    limit 0