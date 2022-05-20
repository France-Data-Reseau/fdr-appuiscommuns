{#
Example de profilage des données d'une source par le profiler de DBT Hub :

On _specific so that it gives insight even on wrong values (ex. commune INSEE id = "ko") NOOOO

TODO plutôt la source sous-jacente
#}

-- depends_on: {{ ref('apcom_osm_supportaerien_deduped') }}
{% if execute %}
  {{ dbt_profiler.get_profile(relation=ref('apcom_osm_supportaerien_deduped')) }}
{% endif %}