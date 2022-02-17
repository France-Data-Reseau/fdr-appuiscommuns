{#
Example de profilage des données d'une source par le profiler de DBT Hub :
#}

-- depends_on: {{ ref('osmgeodatamine_powsupp__appuiscommuns_supportaerien') }}
{% if execute %}
  {{ dbt_profiler.get_profile(relation=ref('osmgeodatamine_powsupp__appuiscommuns_supportaerien')) }}
{% endif %}