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

Exemple :
{"properties" : {"apcomsup_Nature" : null, "apcomoc_occupations_by_gestionnaire" : [{"apcomoc_Gestionnaire":"Orange","occupations":[{"apcomoc_Technologie":"CUIVRE","apcomoc_Reseau":"DI"},{"apcomoc_Technologie":"FIBRE","apcomoc_Reseau":"RA"}]}]}, "type" : "Feature", "geometry" : {"type" : "Point", "coordinates" : [-4.072638, 48.512337]}}

Présentation :
(dans Superset Chart > Tooltip Javascript)

d => `
<div>${d.object.properties.apcomsup_Nature || 'Nature inconnue'}</div>
${(d.object.properties.apcomoc_occupations_by_gestionnaire || []).map(obg => `
<div>${obg.apcomoc_Gestionnaire || 'Gestionnaire inconnu'} :</div>

${(obg.occupations || []).map(occ => `
<div>${occ.apcomoc_Technologie || 'Technolgie inconnue'}, ${occ.apcomoc_Reseau || 'Réseau inconnu'}</div>
`).join("\n")}

`).join("\n")}
`

Limitations :
- le tooltip javascript (et donc Superset Deck.gl) ne permet pas de style (donc de police plus petite pour Technologie et Réseau)
- la symbologie (beton ou bois OU INCONNU, et si coaxial et / ou fibre, cuivre) pourrait être réalisée avec des propriétés
supportées par Deck.gl, soit en mode texte (simple), soit en mode icon (MAIS requiert SOIT une icône pour chaque combinaison
de symbôles donc 3*4=12, SOIT une copie des données par symbôle donc 4, SOIT un mix des 2), voir
https://deck.gl/docs/api-reference/layers/geojson-layer

Developpement :
d => `
d : ${Object.keys(d).join(",")}
d.object : ${Object.keys(d.object).join(",")}
`

#}

{% set fieldPrefix = 'apcomsup' + '_' %}
{% set order_by_fields = [fieldPrefix + 'src_priority', fieldPrefix + 'src_id'] %} -- must include dedup relevancy order

-- TODO on _src_id car IdSupportAerien pas unique globalement !(?)
{{
  config(
    materialized="view",
  )
}}

with selected as (
    select distinct "apcomsup_id", "apcomoc_Gestionnaire", "apcomoc_Technologie", "apcomoc_Reseau"
    from {{ ref('apcom_std_suivioccupation_enriched') }}

), apcomoc_by_sup_ocges as (
    select "apcomsup_id", "apcomoc_Gestionnaire",
        json_strip_nulls(json_agg(json_build_object(
            'apcomoc_Technologie', "apcomoc_Technologie", 'apcomoc_Reseau', "apcomoc_Reseau"
          ))) as apcomoc_occupations_of_gestionnaire
    from selected
    group by "apcomsup_id", "apcomoc_Gestionnaire"

), apcomoc_by_sup as (
    select "apcomsup_id",
        json_strip_nulls(json_agg(json_build_object(
            'apcomoc_Gestionnaire', "apcomoc_Gestionnaire", 'occupations', "apcomoc_occupations_of_gestionnaire"
        ))) as apcomoc_occupations_by_gestionnaire
    from apcomoc_by_sup_ocges
    group by "apcomsup_id"
)

select
    sup.*, -- includes apcomsup_DateConstruction, to avoid weird Superset chart error Time column "apcomsuoc_DebutOccupation" does not exist in dataset
    --json_build_object(
    --    'type', 'FeatureCollection',
    --    'features', json_build_array(
        json_build_object(
            'type', 'Feature',
            'geometry', json_build_object('type', 'Point', 'coordinates', json_build_array(x, y)),
            'properties', json_build_object(
                'apcomsup_Nature', "apcomsup_Nature",
                'apcomoc_occupations_by_gestionnaire', apcomoc_occupations_by_gestionnaire
            )
        )
        --))
        #>> '{}' as geojson_point -- must be text else Superset error could not identify an equality operator for type json LINE 4: GROUP BY json_build_object https://stackoverflow.com/questions/27215216/postgres-how-to-convert-a-json-string-to-text
from apcomoc_by_sup
join {{ ref('apcom_std_supportaerien_unified') }} sup on apcomoc_by_sup."apcomsup_id" = sup."apcomsup_id"