{#
geo indexed version ("staging" version in the words of DBT) of the CKAN imported CSV file.
TODO move it in the fdr_francedatareseau DBT project.
NB. might be auto generated.

geojson version could be kept to not to have to regenerate it for CSV output (TODO generate it everywhere, as in _csv.sql) :
, except=[
      "geo_point_2d",
      "geo_shape"]
#}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['geo_shape_4326'], 'type': 'gist'},]
  )
}}

select
    {{ dbt_utils.star(source('france-data-reseau', 'georef-france-commune.csv'), except=[
      "geo_point_2d",
      "geo_shape"]) }},
    ST_PointFromText('POINT(' || replace(c.geo_point_2d, ',', ' ') || ')', 4326) as geo_point_4326,
    ST_GeomFROMText(ST_AsText(ST_GeomFromGeoJSON(c.geo_shape)), 4326) as geo_shape_4326
from {{ source('france-data-reseau', 'georef-france-commune.csv') }} c