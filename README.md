# Projet dbt appuiscommuns et autres exemples France Data Réseau

Ce projet dbt (Data Build Tool) est théoriquement consacré au cas d'usage Appuis Communs, dans le cadre de l'initiative France Data Réseau. Toutefois, il contient pour l'instant aussi la transformation des données de la source publique OSM Geodatamine power supports (de type URL CSV) indiquée par le partenaire expert et "data steward" Datactivist vers le modèle normalisé de ce cas d'usage, au lieu d'être dans un projet dbt dédié tel fdr_appuiscommuns_osm(powersupports).

Ce que fait ce projet dbt :
- côté source :
 - exemples embarqués : _extract.csv (réel, à ne pas ouvrir sur github ?), __definition.csv (imaginaire)
  - TODO en cours de migration vers --target staging basé dessus, _source.yml dbt à fournir par leur projet dbt en dep ?
 contient des lignes choisies assez complètes du jeu OSM Geodatamine complet
 - Lecture, parsing (géo de X Y WSG84 par PostGIS, sinon integer par dbt seed sauf code INSEE commune sinon 01001 devient 1001, pas de date),
 - renommage : ceux à parser depuis string suffixés par __s avant exploitation
 - pas de nettoyage (car venant d'OSM ; sinon règles SQL sur WHERE, ou bien Excel ou Open Refine),
 - retypage d'exemple embarqué (CSV) et (TODO bug taille vs proxy) de vue sur fichier CSV importé dans CKAN,
 - réconciliation pour traduction de valeurs de dictionnaires (définis dans l_*.csv et liés dans l_*__<source>/mapping.csv),
  - TODO LATER réconciliation approchée entre 2 sources DIFFERENTES ne partageant pas d'id :
   - appuiscommuns : < 5 m entre 2 poteaux signifie que c'est le même
   - méthodologie : pour commencer simple SQL DBT, à terme possibilité de mesure de proximité des termes / valeurs, avec recherche plein texte PostgreSQL voire intégration d'un moteur de déduplication complet, comme Duke (sur le moteur de recherche Lucene) https://github.com/larsga/Duke ou (en Python) https://github.com/dedupeio/dedupe , qui génère, entre entités entre 2 listes ou dans la même, des liens "les meilleurs possibles" à approuver / rajouter dans la base, mais seulement si la complexité d'intégration et de configuration par cas d'usage (champs) et collectivités (règles) en vaut la chandelle.
 - champs calculés à partir des précédents (uuidv5 reproductible un peu comme un hash, traduction de dictionnaire complexe, TODO LATER pour d'autres sources qui n'ont pas de commune, déduction par reverse geocoding),
 - (ou côté exploitation !?!) tests génériques par contraintes de schéma (_schema.yml) dont relationship avec source ET dictionnaire ; TODO de https://github.com/calogica/dbt-expectations
 - pas d'export depuis une base existante (Airbyte, à préférer à FDW, voir Synchronisation de la normalisation d'une base de données existante vers le datalake FDR à https://github.com/France-Data-Reseau/fdr_altereo_hpocollect )
 - outils : TODO BUG profilage
- côté exploitation :
 - NB. à la question "traitements plutôt avant ou après union" dbt répond "après" https://github.com/dbt-labs/dbt-core/issues/2716
 - unification : UNION par dbt_utils (et non explicite), uniquement des champs de la __definition.csv (permettant d'avoir d'autres champs spécifiques à la source dans sa normalisation)
 - (ou côté source si spécifique) enrichissement (des communes ODS et de leurs populations, par left join et non join sinon les lignes sans valeur manquent),
 - exploitation :
   - exemple de calcul d'indicateurs basiques : par commune et région par GROUP BY : count et par habitant, min / max / avg des nombres, valeurs distinctes des dictionnaires (par array_agg distinct) et compte pour chacun (par macro dbt pivot), TODO aussi leur pourcentage (en adaptant la macro dbt)
- côté doc & métamodèle :
 - un nommage en amélioration (org_type_version et contenant éventuellement "sample" ou "extract", prefix__field, prefix__targetPrefix__targetField pour liens ; encore à améliorer y compris arbre des dossiers) ; TODO nommage générique pour faciliter l'adaptation pour une source
   - flacombe : TODO osmgeodatamine => osm, appuiscommuns => appcomm, enlever powsup
 - métamodèle : sous forme de (doc et) tags (voire meta : pii...) dans _schema.yml (normalization, id / unique / uuid, sample / extract) et seeds CSV (type_definition, list, link ou relationship), et comment les générer et les exploiter (dbt docs generate puis serve et les entrer dans le champ de Recherche en cochant "Tags", ou target/catalog.json puis output textuel .md ou alimenter une IHM ex. React.js permettant de naviguer par tag voire schema / org(dataset) et type)
 - TODO doc dbt-style au-delà. Semble limité : description dans les .yml, alimentables depuis {% docs table_events %}{% enddocs %} dans .md, notamment __overview__
 - TODO indicateurs de complétude et qualité, générique et métier (à l'aide de tout le précédent : profilage, tests...)


**TODO**s :
- joli print auto .md de tout ce qu'il y a / sources et chaque étape (donc ont dossiers dédiés ?! pour chacune, puis pour les exploitations plus ou moins génériques ou spécifiques, et __definition.csv plus dicts.csv, voire déduire de profilage les types, formats et caracs ex. id) et plus à fond si formalisé, le mettre dans le README. A partir de target/catalog.json dont les tags?
 - et / ou essayer de générer le métamodèle yaml ! partiellement au moins, depuis ça
 - et / ou le tableau matrice d'adoption des fonctionnalités et des étapes du processus de traitement par CU par les collectivités !
 - prototyper l'output de tout ça / un "ls" de ce projet, en .md dans ce README : (OU en tags et meta dans _schema.yml !!) :
  - modèle :
   - indicateurs :
   - (chaque) type : de *__definition.csv ou union modele/ ou all/
   - distinguer ceux qui sont listes statiques : de ex. l_*.csv SAUF l_*__<source>.csv qui sont liens de réconciliation / relations
- refactoring et mutualisation :
 - extraction des éléments des sources vers autant de projets qui en dépendent ((fdr_)appuiscommuns_osm) ou plusieurs (fdr, et le partager avec les autres projets dbt)
 - peut-être des exemples en sous-projet integration_tests (vu dans https://github.com/ozwillo/dbt-profiler ). Essayer de faciliter sa prise en main : pas de mise en place préalable (base source) ou la faire une fois pour tout le monde...
 - org(_dataset)_source.yml : dans les projets dbt éponymes en dep, les tables / views SQL qu'il offre. Pourrait aussi inclure la création des vues sur les tables créée à l'import par CKAN avant l'automatisation ? voire l'automatisation par introspection SQL de la base CKAN (ou dans un projet dbt uniqué dédié) ??
 - union : générique par get_relations_by_pattern(schemas pas test, '%__casdusage_type.sql') OU jinja graph nodes selectattr filter https://docs.getdbt.com/reference/dbt-jinja-functions/graph https://oznetnerd.com/2017/04/18/jinja2-selectattr-filter/ (mais plus à fond pas encore d'actualité https://github.com/dbt-labs/dbt-core/issues/1212 https://github.com/dbt-labs/dbt-core/issues/2716 ) OU dbt core dev. 
 - tout côté exploitation : aussi dans projet dep ? permettrait de les avoir séparément pour cette source dans son schema (mais c'est peut-être mieux de les avoir dans la même table avec une colonne "source")
 - tout côté source : aussi dans projet dep ? permettrait d'avoir les exemples embarqués (mais sont déjà dans le projet source base), pourraient être référencés par _source.yml (ou remplacés si conf prop activée)
 - quels autres dbt models .sql dans projet dep : indicateurs de complétude et qualté etc. !
 - et ceux qu'on ne veut pas peuvent être désactivés, tous ou dans un dossier https://github.com/dbt-labs/dbt-core/issues/3043 ! (par ailleurs, ils peuvent être dans un autre schema, avoir des tags...)
 - Q comment mettre à jour la "mécanique" d'un projet de source (ou...) depuis son projet de base ? il faudrait qu'elle soit toute entière dans le projet dep ! et seuls les overrides et remplissage de CSV etc. dans le projet réel ?! MAIS pour l'instant pas d'overrides ( https://github.com/dbt-labs/dbt-core/issues/4157 , les seuls sont overrides sont de source https://docs.getdbt.com/reference/resource-properties/overrides ) donc simplement désactivés les models de dep à overrider et les copier et modifier dans le projet source.
- ouvrir les projets github, après élimination / démarquage de toutes données réelles
- nommage plus global et cohérent :
 - des fichiers : models (sample/src__fournisseur_source_typeunifie_version.sql), description de source (fournisseur_nom_source.yml), tests génériques de contrainte de schema ((fdr_?)casdusage_schema.yml), exploitation (perimetre__monexploitation.sql), profilage
 - dossiers : trouver une meillere organisation des fichiers
 - par étape du processus de traitement des données (source, normalized, unified, exploitation...) ?
 - supportant un seul projet dbt pour une source alimentant plusieurs cas d'usage ?
 - supportant au sein d'un seul projet dbt de la source ET de l'exploitation, par exemple par fournisseur (expert data steward) ou pour des tests ?
- code python pour : alimenter la base datalake (depuis fichiers mis en ligne sur CKAN) ou préparer, exploiter en déhors de la base (ou avec fal https://github.com/fal-ai/fal )
- réconciliation et dictionnaires de valeurs : les rentrer plutôt par CKAN ?? (mais serait moins bien versionné)
- contraintes : relationship pour référence ou dictionnaire, https://github.com/calogica/dbt-expectations (pour le modèle des tests plus élargis de nom et type de colonnes, valeurs dans intervalle ou ordre ou regex ou liste au lieu de relationship, agrégats et proportions...)
- utilisation d'une source tableur mise dans le datalake par CKAN, devant être visbilisée par une view SQL
- enrichissement ex. depuis communes ODS, devant être visibilisée par une view SQL
- unification : dbt_utils.union_relations(), auto de toutes les relations ex. dans schemas des organisations CKAN dont un cas d'usage dépend hors tests ou selon un métamodèle
- doc :
 - screenshots
 - outil SQL ex. DBeaver,
 - guide / tutorial plus précis de techniques pour comment lire / analyser / introspecter, parser, nettoyer (du mail à Etienne), retyper, normaliser, vérifier une source
 - manuel de comment bien adopter / adapter / (ré)utiliser le(s) projet(s) dbt de base / exemple et mutualisés
  - TODO outiller : génération de CSV (ex. _expected.csv..., mais plutôt pas __definition.csv qui doit être fabriqué et non réel) depuis relation SQL model DBT de source nettoyée voire unifiée
   - DBeaver :
    - sur les données, bouton droit > Exporter les résultats... et dans la fenêtre modale : Exporter en fichier CSV,
    - Suivant : Fetch size = 5 (pour un exemple, ou ex. 100000 pour tout),
    - Suivant : optionellement changed Séparateur à ; si on préfère Excel,
    - Suivant : si besoin changer le nom du fichier, Suivant : commencer
- datalake : tableau des logins/pass, organisation et IP ; automatisations


## Install, build & run

**IMPORTANT** après dbt deps, remplacer le contenu de dbt_packages/dbt_profiler par celui de https://github.com/ozwillo/dbt-profiler (attention au "_", ne sera plus nécessaire quand une nouvelle version aura été publiée incluant https://github.com/data-mie/dbt-profiler/pull/38 )

### Install DBT (1.0)

comme à https://docs.getdbt.com/dbt-cli/install/pip :
(mais sur Mac OSX voir https://docs.getdbt.com/dbt-cli/install/homebrew )

```shell
sudo apt-get install git libpq-dev python-dev python3-pipsudo apt-get remove python-cffisudo pip install --upgrade cffipip install cryptography~=3.4

sudo apt install python3.8-venv
python3 -m venv dbt-env
source dbt-env/bin/activate
pip install --upgrade pip wheel setuptools
pip install dbt-postgres
# mise à jour :
#pip install --upgrade dbt-postgres
```

### Configuration

comme à https://docs.getdbt.com/dbt-cli/configure-your-profile :

```
mkdir /home/mdutoo/.dbt
vi /home/mdutoo/.dbt/profiles.yml
# nom du profile : fdr_votreorganisation(_votrebase)
# à adapter sur le modèle du profiles.yml fourni
```

Serveur PostgreSQL FDR (base partagée datalake ou dédiée) : demander

(optionnel) serveur PostgreSQL local :

Create "fdr" user :

    $> sudo su - postgres
    $> psql
    $postgresql> create user fdr with password 'fdr' createdb;
    $postgresql> \q

Create "fdr_datalake" database :

        $> psql -U fdr postgres -h localhost
        $postgresql> create database fdr_datalake encoding 'UTF8';

Now you should be able to log in to the database with the created user :

        psql -U fdr fdr_datalake -h localhost
        
### Build & run
        
```shell
source dbt-env/bin/activate
dbt deps # (une seule fois) installe les dépendance s'il y en a dans dbt_packages depuis https://hub.getdbt
dbt debug # (une seule fois) pour vérifier la configuration
dbt seed # (--full-refresh) quand les données d'exemple embarquées dans seeds/ changent
dbt run # (--full-refresh) pour réinstaller les models dans la base cible en tables et views
dbt test # pour exécuter les tests, d'une part génériques (de contraintes sur schémas configurées en .yml, notamment pour les modèles normalisés), et d'autre part spécifiques (requêtes dans des fichiers .sql sous tests/ renvoyant une ligne par erreur trouvée)
dbt docs generate
dbt docs serve # (--port 8001) sert la doc générée sur http://localhost:8000
```

