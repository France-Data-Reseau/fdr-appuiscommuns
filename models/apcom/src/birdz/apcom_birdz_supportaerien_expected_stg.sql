{#
Parsing de l'attendu _expected du test unitaire de normalization

La normalisation birdz est utilisée comme définition des colonnes non dans le modèle normalisé
(pas idéal mais évite de retyper tous les champs du CSV _expected)

__expected and NOT _definition else also asks for the fields not provided by this source
TODO generate
'birdz__apcom_supportaerien__expected'
#}

{{
  config(
    materialized="view"
  )
}}

{{ fdr_francedatareseau.from_csv(ref(model.name[:-4]), [ref('apcom_def_supportaerien_definition'), ref('apcom_birdz_example_stg')], wkt_rather_than_geojson=true) }}
{# , best_geometry_columns=['geom', 'geometrie'], target_geometry_column_name='geometry' #}