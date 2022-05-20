{#
Parsing de l'attendu _expected du test unitaire de normalization

La normalisation birdz est utilisée comme définition des colonnes non dans le modèle normalisé
(pas idéal mais évite de retyper tous les champs du CSV _expected)

__expected and NOT __definition else also asks for the fields not provided by this source
TODO generate
#}

{{
  config(
    materialized="view"
  )
}}

{{ from_csv(ref(model.name[:-4]), [ref('appuiscommuns_supportaerien__definition'), ref('megalis__apcom_supportaerien')], wkt_rather_than_geosjon=true) }}