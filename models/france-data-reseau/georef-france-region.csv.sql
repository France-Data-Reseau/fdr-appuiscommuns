{#
geo indexed version ("staging" version in the words of DBT) of the CKAN imported CSV file.
TODO move it in the fdr_francedatareseau DBT project.
NB. might be auto generated.

indexes=[{'columns': ['geo_shape_4326'], 'type': 'gist'},]
#}

{{
  config(
    materialized="table",
    
  )
}}

select
    {{ dbt_utils.star(source('france-data-reseau', 'georef-france-region.csv'), except=[
      "Geo Point",
      "Geo Shape"]) }},
    ST_PointFromText('POINT(' || replace(c."Geo Point", ',', ' ') || ')', 4326) as geo_point_4326,
    ST_GeomFROMText(ST_AsText(ST_GeomFromGeoJSON(c."Geo Shape")), 4326) as geo_shape_4326
from {{ source('france-data-reseau', 'georef-france-commune.csv') }} c