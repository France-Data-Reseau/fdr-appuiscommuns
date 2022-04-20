{#
Example de profilage des données d'une source par le profiler de DBT Hub :

On _specific so that it gives insight even on wrong values (ex. commune INSEE id = "ko") NOOOO
#}

-- depends_on: {{ ref('osm_powsupp__appuiscommuns_supportaerien') }}
{% if execute %}
  {{ dbt_profiler.get_profile(relation=ref('osm_powsupp__appuiscommuns_supportaerien')) }}
{% endif %}