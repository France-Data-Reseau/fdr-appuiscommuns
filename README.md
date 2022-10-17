# Projet dbt appuiscommuns et autres exemples France Data Réseau

Ce projet dbt (Data Build Tool) est consacré au cas d'usage Appuis Communs, dans le cadre de l'initiative France Data Réseau.

See Install, build & run and FAQ / Gotchas in fdr-france-data-reseau.

Regular (incremental) run :
```bash
dbt run --target prod --select apcom.src tag:incremental
```

## Provides

- models :
  - apcom_kpi_* :
    - _suivioccupation_alerte_fin(_commune_owner) : indicateurs sur suivioccupation (fin occupation, redevance traverse),
et version agrégée par commune et gestionnaire / data_owner_id (rajouter dépose cuivre, déploiement fibre,
pivots selon materiau, exploitant, occupant, technologie, réseau convention)
    - _suivioccupation_day(_commune_owner) : indicateurs d'évolution journalière (mais agrégeable dans Superset
ex. annuellement pour redevance) sur suivioccupation (fin occupation, redevance traverse),
et version agrégée par commune et gestionnaire / data_owner_id (rajouter dépose cuivre, déploiement fibre,
pivots selon materiau, exploitant, occupant, technologie, réseau convention)
    - _supportaerien(_commune_owner) : indicateurs basiques sur supportaerien, éventuellement agrégé par commune et gestionnaire / data_owner_id
  - apcom_std_* :
    - _unified (tables, incrémentales) : modèle d'échange i.e. données normalisées et unifiées entre sources
    - _enriched : version enrichie par jointure interne et rapprochement des communes
    - _map : produit le geojson pour cartographie dans Superset Deck.gl
    - _commune_linked, _dedupe_candidates (tables, incrémentales) :  stockent les résultats des rapprochements et déduplication
  - apcom_src_* : translations for
    - osm (supportaerien),
    - birdz (supportaerien, equipement),
    - gthdv2 (supportaerien),
    - native apcom (all types)
  - apcom_def_* : definition & examples (see example data in seeds/) for types supportaerien, equipement, occupation, suivioccupation
- macros :
  - near geometry deduplication (and example ARRAY linking)
  - translation : gthdv2, osm
  - apcom helpers