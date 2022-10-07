{#
Carte géographique des supports avec leurs suivis d'occupation, du JDB :
explication de l'image à https://jdb.francedatareseau.fr/groupes-projets/appuis-communs/cadrage#restitution-cartographique :
- Poteau béton => apcomsup_Nature
- puis dessous, group by sur exploitant télécom :
- Orange => apcomoc_Gestionnaire exploitant télécom
  - Cuivre distribution => apcomoc_Technologie, apcomoc_réseau
- RIP FTTH => 2e exploitant télécom ! qui a 2 apcomoc :
  - Fibre distribution
  - Fibre raccordement
- à côté du poteau, pointS : rouge = cuivre, vert = fibre
#}

{% set fieldPrefix = 'apcomsup' + '_' %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

-- TODO on _src_id car IdSupportAerien pas unique globalement !(?)
{{
  config(
    materialized="view",
  )
}}

select
--*,
{{ dbt_utils.star(ref('apcom_std_suivioccupation_enriched'), except=['x', 'y']) }}, --
ST_X(geometry) as x, ST_Y(geometry) as y,
    --json_build_object(
    --    'type', 'FeatureCollection',
    --    'features', json_build_array(
        json_build_object(
            'type', 'Feature',
            'geometry', json_build_object('type', 'Point', 'coordinates', json_build_array(x, y)),
            'properties', json_build_object('apcomsup_Nature', "apcomsup_Nature",
                'apcomoc_Gestionnaire', "apcomoc_Gestionnaire",
                'apcomoc_Technologie', "apcomoc_Technologie", 'apcomoc_Reseau', "apcomoc_Reseau",
                'test', json_build_array(json_build_object('a', 1, 'b', 2)))
        )
        --))
        #>> '{}' as geojson_point -- must be text else Superset error could not identify an equality operator for type json LINE 4: GROUP BY json_build_object https://stackoverflow.com/questions/27215216/postgres-how-to-convert-a-json-string-to-text
from {{ ref('apcom_std_suivioccupation_enriched') }}
--group by "apcomoc_Gestionnaire"