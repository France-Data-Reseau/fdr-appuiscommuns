# Projet dbt appuiscommuns et autres exemples France Data Réseau

Ce projet dbt (Data Build Tool) est théoriquement consacré à la normalisation des données de la source publique OSM Geodatamine power supports (de type URL CSV) indiquée par le partenaire expert et "data steward" Datactivist vers le modèle normalisé du cas d'usage Appuis Communs, dans le cadre de l'initiative France Data Réseau.
Toutefois, il contient aussi des exemples plus larges de la mise en oeuvre appropriée de dbt pour les différents besoins du projet France Data Réseau, dans une première version très simple. Ces exemples ont vocation à être étendus, détaillés, et pour beaucoup déplacés dans leur bon projet dbt.
- côté source : Lecture, parsing (géo de X Y WSG84 par PostGIS, sinon integer par dbt seed, pas de date), pas de nettoyage (venant d'OSM ; sinon règles SQL sur WHERE, ou bien Excel ou Open Refine), retypage d'exemple embarqué (CSV) et TODO de vue sur fichier CSV importé dans CKAN, réconciliation pour traduction de valeurs de dictionnaires (définis dans l_*.csv et liés dans l_*__<source>.csv), champs calculés à partir des précédents (traduction de dictionnaire complexe), tests génériques par contraintes de schéma dont relationship avec source ET dictionnaire, TODO BUG profilage, pas d'export depuis une base existante (Airbyte, voire FDW)
- côté exploitation : unification (UNION par dbt_utils uniquement des champs de la __definition.csv), enrichissement (des communes ODS), exploitation par calcul d'indicateurs basiques (GROUP BY).
- côté doc & métamodèle : un nommage en amélioration (org_type_version et contenant éventuellement "sample" ou "extract", prefix__field, prefix__targetPrefix__targetField pour liens ; encore à améliorer y compris arbre des dossiers), des tags voire meta dans _schema.yml (normalization, id / unique / uuid, sample / extract) et seeds CSV (type_definition, list, link ou relationship), et comment les générer et les exploiter
- et : en cours de migration vers --target staging basé sur CSV samples

**TODO**s :
- joli print auto .md de tout ce qu'il y a / sources et chaque étape (donc ont dossiers dédiés ?! pour chacune, puis pour les exploitations plus ou moins génériques ou spécifiques, et __definition.csv plus dicts.csv, voire déduire de profilage les types, formats et caracs ex. id) et plus à fond si formalisé, le mettre dans le README
- et / ou essayer de générer le métamodèle yaml ! partiellement au moins, depuis ça
- et / ou le tableau matrice d'adoption des fonctionnalités et des étapes du processus de traitement par CU par les collectivités !
- prototyper l'output de tout ça / un "ls" de ce projet, en .md dans ce README : (OU en tags et meta dans _schema.yml !!)

- modèle :
 - indicateurs :
 - (chaque) type : de *__definition.csv ou union modele/ ou all/
 - distinguer ceux qui sont listes statiques : de ex. l_*.csv SAUF l_*__<source>.csv qui sont liens de réconciliation / relations

TODO outiller : génération de __definition.csv depuis relation SQL model DBT de source nettoyée voire unifiée
DBeaver : sur les données, bouton droit > Exporter les résultats... et dans la fenêtre modale : Exporter en fichier CSV,
Suivant : Fetch size = 5 (pour un exemple, ou ex. 100000 pour tout),
Suivant : optionellement changed Séparateur à ; si on préfère Excel,
Suivant : si besoin changer le nom du fichier, Suivant : commencer

**TODO** mettre à jour tout ci-dessous

**TODO**s globaux :
- extraction des éléments des sources vers autant de projets qui en dépendent ((fdr_)appuiscommuns_osm) ou plusieurs (fdr, et le partager avec les autres projets dbt)
- peut-être des exemples en sous-projet integration_tests (vu dans https://github.com/ozwillo/dbt-profiler ). Essayer de faciliter sa prise en main : pas de mise en place préalable (base source) ou la faire une fois pour tout le monde...
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
- doc dbt-style . Semble limité : description dans les .yml, alimentables depuis {% docs table_events %}{% enddocs %} dans .md, notamment __overview__
- doc : screenshots, outil SQL ex. DBeaver, guide / tutorial plus précis de techniques pour comment lire / analyser / introspecter, parser, nettoyer (du mail à Etienne), retyper, normaliser, vérifier une source
- datalake : tableau des logins/pass, organisation et IP ; automatisations


## Ce que fait ce projet dbt :

### Cadre

En général, un projet dbt FDR cible la base du datalake PostgreSQL ("datastore"), avec comme schema
- pour un projet de normalisation de source : votreorganisation(_votresource). Cette source est des données automatiquement importées dans la base par CKAN (fichiers structurés de tableur) ou par du code (python) depuis des fichiers mis en ligne sur CKAN.
- pour un projet d'unification et exploitation de cas d'usage : fdr_casdusage.

Toutefois, un projet dbt de normalisation des données d'une source de type base de données (tel que le présent projet avec la source Altereo HpO Collect) ne peut **s'exécuter que dans une base de données dédiée** à celle-ci, et donc pas dans la base du datalake. Car aucune alternative d'export de base est satisfaisante :
- **pg_dump/pg_restore : la seule manière de d'exporter une base parfaitement dans une autre**. Mais ne permet pas d'en changer quoi que ce soit (schema "app_public" dans le cas de la base Altereo HpoCollect, ou utilisateur, ou bien sûr nom et types des tables) hors solution spécifique (remplacement approprié dans le dump SQL), ce qui polluerait la base datalake ou y serait interdit.
- PostgreSQL FDW (Foreign Data Wrapper) (voir plus bas) : permet de visibiliser tables et views dans une autre base ET un autre schéma, MAIS ne supporte pas les types de données non standards, et au premier chef les types ENUM. Donc ça marche bien sur des données déjà "massées", mais pas sur des données applicatives orginelles qui en ont souvent. De plus, la mise en place est plus complexe, nécessite des privilèges et de la gestion (mise à jour du schéma).
- Airbyte (voir plus bas) : le seul changement permis est de préfixer les noms des tables, mais le schema n'est pas changé et donc s'il n'est "en dur" (notamment si ce n'est pas le schema par défaut) c'est problématique. Mais il est d'usage simple et visuel. C'est donc **la bonne solution pour exporter des données de base une fois normalisées vers leur lieu d'exploitation dans le datalake**, à condition que ce projet les ait déjà produites déjà dans le **bon schema** : votreorganisation(_votresource).

La bonne solution est donc que le projet dbt de normalisation
- soit déployé dans une base dédiée copie exacte (par pg_dump/pg_restore) de la base source, ou la base source elle-même
- et ait pour target schema celui que l'on souhaitera dans le datalake (votreorganisation(_votresource)), ce qui permet de configurer une copie des bases produites par dbt par Airbyte au bon endroit dans la base datalake.

### Source exemple de données embarqué, parsée, nettoyée et retypée, normalisée "geodatamine_power_supports_extract.csv"

- CSV dans seeds/ importé par dbt seed (contient des lignes choisies assez complètes du jeu OSM Geodatamine complet)
- sample__appuiscommuns_extract_supportaerien.sql : Normalisation vers le modèle de données du cas d'usage des données de type supportaerien de la orécédente source d'exemple embarquée
 - renomme les champs : de manière compréhensibles, ceux à parser depuis string suffixés par __s
 - parse les types avancés depuis string : date et decimal (avec des fonctions "lenient" si null / non renseigné), geometry
 - nettoie : différentes stratégies de parsing sont utilisées, par exemple différents formats de date
 - les retype : de ces différentes stratégies sont tirées la meilleure valeur possible pour chaque champ

**TODO** :
- exemple de source : ce fichier n'est pas cohérent. Il faut le séparer en un fichier par commune, chacun avec des erreurs cohérentes entre elles, ou au moins cohérentes selon un autre champ indiquant leur source originelle (par exemple un logiciel, un auteur, une campagne de saisie), et enfin avec une sémantique aussi réaliste que possible ("Paris" et non "commune2" par exemple)
- nettoyage : cela permettra des stratégies de nettoyage par fichier ou au moins par source originelle, donc plus exhaustives (que ce soit par dbt / SQL dans ce projet, ou par un outil externe comme Excel ou Open Refine)
- modèle normalisé cible "eaupotable" : cela permettra d'en appliquer et vérifier davantage de contraintes
- indicateurs "eaupotable" : cela permettra d'avoir des exemples d'indicateurs plus parlants

### Source base de données existante, normaliséee "HpO Collect"

- description : dans altereo_hpocollect_source.yml, au moins chaque table lue
- src__altereo_hpocollect_canalisation_20211116.sql : Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source Altereo Hpo Collect, sur le périmètre minimal défini par l'exemple "echantillon 3"
 - renomme les champs : d'anglais en français
 - les parse : quand nécessaire les valeurs des valeurs de dictionnaire ; époque de pose vers date, valeur vers numeric
 - les retype : de ces valeurs sont tirées la meilleure valeur possible pour chaque champ

### Exemple de profilage, de données de source base ou bien produites par DBT

Le profilage de données permet de mieux les connaître.
- Dans le cas de données source, il aide donc le travail de nettoyage et normalisation.
- dans le cas de données produites par DBT, il aide ceux qui vont les exploiter, dans tous les cas dans le projet cas d'usage : les développeurs de l'unification voire l'enrichissement dans le cas de données normalisées, ou les développeurs d'exploitation et indicateurs dans le cas de données unifiées et enrichies.

Exemple de profilage des données
- par le dbt-profiler (de DBT Hub), du dictionnaire pipe_material, et au fil du temps (par materialized=incremental ; avantage de dbt-profiler)
- par le profiler porté depuis BigQuery de rittmananalytics.com, sur toutes les tables (avantage de cette solution, par ailleurs plus adaptée aux sources brutes car essaie de caster les string vers numeric pour ses indicateurs, mais par contre a quelques imperfections) la base source (schema app_public) ET les données produites par DBT
- comparatif de ces solutions : chacun des avantages de l'un pourrait facilement être porté vers l'autre, sauf peut-être ceux qui viennent de l'approche "json" du 2e. Mais mieux vaut une solution officielle pour la pérennité, quitte à l'enrichir, surtout que cela a déjà été fait : https://github.com/data-mie/dbt-profiler/pull/38

**TODO** :
- obtenir de retours sur les vrais besoins dans ce domaine, enrichir en conséquence le DBT profiler

### Tests génériques de contraintes de schéma "eaupotable" appliqués aux précédentes

eaupotable_schema.yml :
- contraintes minimalistes pour que ça passe malgré les erreurs de la source exemple échantillon 3

**TODO** :
- rajouter des contraintes au fur de l'amélioration des données des sources (données originelles ET nettoyage plus agressif et réaliste), notamment echantillon 3 comme dit
- et notamment des contraintes https://github.com/calogica/dbt-expectations

### Synchronisation de la normalisation d'une base de données existante vers le datalake FDR

par Airbyte (conseillé) ou sinon PostgresFDWW, voir plus bas.

### Unification

eaupotable_canalisation_20220114.sql :
- unification de la normalisation des sources actuelles (base Altereo HpOCollect, exemple echantillon 3)

### Enrichissement

NB. cette partie pourrait sinon être
- soit avant Unification, si elle permet d'obtenir des données plus complètes dans le cas d'une source en particulier, et notamment pour répondre aux exigences du modèle de données normalisées. Si d'autres sources ont le même besoin précis, cela pourrait être mutualisé, soit au sein d'un projet aidant à la normalisation de ce cas d'usage, soit dans l'unification même de nouveau.
- soit spécifique à une exploitation (indicateur...) particulière, si elle est nécessaire pour celle-ci mais a priori pas d'autres. Si d'autres exploitations ont le même besoin précis, cela pourrait être mutualisé, soit au sein d'un projet aidant à l'exploitation de ce cas d'usage, soit dans l'unification même de nouveau.

**TODO** :
- enrichissement de la view des communes ODS du CKAN, une fois qu'il y a des données source assez bonnes

### Example d'exploitation - calcul d'indicateurs agrégés classiques, par commune

eaupotable_canalisation__indicators.sql :
- min et max, de date et numeric
- ensemble des valeurs rencontrées (dans une commune donc), pour une valeur de dictionnaire


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
fdr_altereo:
  target: staging
  outputs:
    dev:
      # serveur PostgreSQL local (optionnel, à installer et configurer comme indiqué) :
      type: postgres
      host: localhost
      user: fdr
      password: fdr
      port: 5432
      dbname: fdr
      schema: dbt_fdr
      threads: 4
  outputs:
    staging:
      # serveur PostgreSQL datalake partagé (demander un accès) :
      type: postgres
      host: 141.94.244.102
      user: "you"
      password: "..."
      port: 55432
      # base et schema :
      # soit datastore et votreorganisation(_votrebase) dans le cas général de visibilisation de données déposées sur CKAN :
      #dbname: datastore
      #schema: altereo
      # soit dans le cas d'une visibilisation de base applicative, ladite base et un schema dbt classique :
      dbname: altereo_hpocollect_20211116
      schema: dbt_you
      threads: 4
```

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


## Synchronisation de la normalisation d'une base de données existante vers le datalake FDR

### Par Airbyte - Install

https://docs.airbyte.com/quickstart/deploy-airbyte
```shell
git clone https://github.com/airbytehq/airbyte.git
cd airbyte
docker-compose up -d
```

si mise à jour :
```shell
docker-compose down --volumes
docker-compose up --build -d
```
sinon erreur PostgreSQL Database directory appears to contain a database; Skipping initialization

### Par Airbyte - Synchronisation (à préférer)

Comme on l'a dit, la normalisation d'une base de données existante est nécessairement dans une base dédiée et non dans le datalake FDR. Il faut donc l'y mettre. On le fait ici à l'aide de l'outil EL(T) visuel Airbyte.

Définition de la source :
- Name : src_altereo_hpocollect_20211116 (par exemple)
- (Source type, Host & Port, User & Password : ceux de la base dédiée à la source. Donc ceux du datalake si elle y a déjà été dump & restore, voir exemple plus bas)
- DB name : altereo_hpocollect_20211116

Définition de la destination :
- Name : fdrdatalake_altereo (le schema n'entre pas en ligne de compte dans Airbyte)
- (Destination type, Host & Port, User & Password : ceux du datalake avec un utilisateur Altereo : Postgres, 141.94.244.102, 55432, "youruser", "yourpassword")
- DB Name : datastore
- schema : celui produit par le projet dbt, ici ce n'est que le schema par défaut
- default schema : altereo (ou altereo_hpocollect, ou pourquoi pas altereo_eaupotable pour plus de lisibilité extérieure)
- (manual, Mirror source structure, full refresh)
- basic normalizations, no transformations though we could specify dbt to transform to mmodèle normalisé si on voulait !!!
- table prefix : aucun, les tables produites par dbt doivent déjà avoir été bien nommées et préfixées (altereo_hpocollect_)

Synchronisation :
- cliquer sur "sync now"
- permet ensuite aussi de planifier, et regarder l'état et les logs

**TODO** :
- screenshots
- autoriser "Connect using SSL" dans source et destination
- exemple de planification...

## (déconseillé)) Par l'extension PostgreSQL FDW (Foreign Data Wrapper)

On importe dans un schema particulier et plus approprié (best practice - comme celui d'un dataset d'une organiaation) :
https://stackoverflow.com/questions/61999320/pre-fix-the-foreign-table-with-the-schema-postgres-pwd

Configuration de FDW pour chaque base source :

```shell
create extension postgres_fdw; -- specific to THIS database ;_;
create server local__altereo__hpo_collect_20211116 foreign data wrapper postgres_fdw options (host 'localhost', dbname 'altereo__hpo_collect_20211116'); -- for EACH foreign database ;_;
grant all on schema altereo__hpo_collect_20211116 to altereo;
CREATE USER MAPPING for altereo SERVER local__altereo__hpo_collect_20211116 OPTIONS (user 'altereo', password_required 'false'); -- user required (with login permission !) else postgres ; password '...' not required anymore : https://www.percona.com/blog/2020/09/30/postgresql_fdw-authentication-changes-in-postgresql-13/ else ERROR:  password is required, DÉTAIL : Non-superusers must provide a password in the user mapping.
--grant usage/all on FOREIGN DATA WRAPPER postgres_fdw to app_user;
GRANT ALL ON FOREIGN SERVER local__altereo__hpo_collect_20211116 TO altereo; -- rather than GRANT USAGE because it's his own
GRANT CREATE ON DATABASE datastore TO altereo; -- to grant create schema, required
-- as altereo (NOT e.pequignot) :
--DROP SCHEMA IF EXISTS altereo__hpo_collect_20211116 CASCADE; 
CREATE SCHEMA altereo_hpocollect_20211116 ;
GRANT ALL ON SCHEMA "altereo_hpocollect_20211116" TO "altereo";
```

Visibilisation du schema source dans une autre base par FDW :

```shell
IMPORT FOREIGN SCHEMA app_public FROM SERVER local__altereo__hpo_collect_20211116 INTO altereo_hpocollect_20211116; -- LIMIT TO (land, land2)

ERROR:  schema "app_public" does not exist
LIGNE 3 :   type app_public.legal_person_type OPTIONS (column_name 'ty...
                 ^
REQUÊTE : CREATE FOREIGN TABLE legal_person (
  id uuid OPTIONS (column_name 'id') NOT NULL,
  type app_public.legal_person_type OPTIONS (column_name 'type'),
  representative_id uuid OPTIONS (column_name 'representative_id')
) SERVER local__altereo__hpo_collect_20211116
OPTIONS (schema_name 'app_public', table_name 'legal_person');
CONTEXTE : importing foreign table "legal_person"
```
=> FDW ne permet pas (et sans doute jamais) d'importer des types customs ;_; https://stackoverflow.com/questions/52045255/import-foreign-type-with-postgresql-fdw/52063528

```shell
select enum_range(null::app_public.legal_person_type); -- https://stackoverflow.com/questions/9535937/is-there-a-way-to-show-a-user-defined-postgresql-enumerated-type-definition
     enum_range      
---------------------
 {FIRM,COLLECTIVITY}
(1 ligne)
```
=> notamment, les enums sont des types et doivent donc être transformés en foreign key (aussi une bonne pratique sémantique) ou a minima supprimés (garder la valeur seulement), et ce DANS LA BASE SOURCE (ou par EL(T) mais avec leurs limites interdisent de copier directement vers le datalake partagé FDR)