{#

#}

{{
  config(
    materialized="incremental",
    unique_key='"Libellé"',
    tags=["incremental"],
  )
}}

select * from {{ ref('apcom_src_index_redevance_mois_parsed') }}

{% if is_incremental() %}
  where last_changed > (select coalesce(max(last_changed), to_timestamp('1970-01-01T00:00:00', 'YYYY-MM-DD"T"HH24:MI:SS')) from {{ this }})
{% endif %}