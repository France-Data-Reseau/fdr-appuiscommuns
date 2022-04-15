{#
geo indexed version ("staging" version in the words of DBT) of the CKAN imported CSV file.
TODO move it in the fdr_francedatareseau DBT project.
NB. might be auto generated.
#}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['geo_shape'], 'type': 'gist'},]
  )
}}

select
    {{ dbt_utils.star(source('france-data-reseau', 'georef-france-commune.csv'), except=[
      "geo_point_2d",
      "geo_shape"]) }},
    ST_PointFromText('POINT(' || replace(c.geo_point_2d, ',', ' ') || ')', 4326) as geo_point_2d,
    ST_GeomFROMText(ST_AsText(ST_GeomFromGeoJSON(c.geo_shape)), 4326) as geo_shape
from {{ source('france-data-reseau', 'georef-france-commune.csv') }} c