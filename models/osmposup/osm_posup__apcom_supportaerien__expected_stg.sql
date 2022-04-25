{#
Parsing de l'attendu _expected du test unitaire de normalization

__expected and NOT __definition else also asks for the fields not provided by this source
TODO generate
'osm_powsup__apcom_supportaerien__expected'
#}

{{
  config(
    materialized="view"
  )
}}

{{ from_csv(ref(model.name[:-4]), [ref('appuiscommuns_supportaerien__definition')], wkt_rather_than_geosjon=true) }}