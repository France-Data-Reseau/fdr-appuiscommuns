# Projet dbt appuiscommuns et autres exemples France Data Réseau

Ce projet dbt (Data Build Tool) est théoriquement consacré au cas d'usage Appuis Communs, dans le cadre de l'initiative France Data Réseau. Toutefois, il contient pour l'instant aussi la transformation des données de la source publique OSM Geodatamine power supports (de type URL CSV) indiquée par le partenaire expert et "data steward" Datactivist vers le modèle normalisé de ce cas d'usage, au lieu d'être dans un projet dbt dédié tel fdr_appuiscommuns_osm(powersupports).

TODOs : vars, macroter pour sources ; birdz, refactoring... en fond : incremental (if incremental where updated >), snapshot (SCD2 : rajoute colonnes dbt_valid_from  dbt_valid_to MAIS pas trop en Open Data ; sinon publier par vue liant vers concretS, qui / selon indicateurs voire alertes, lançables depuis Jenkins ?)

TODO montrer :
- principes incremental, snapshots
- TODO Q IRVE


## WHATSNEW

### 20220215

- données réelles :
  - parsing : hauteur devait être nettoyée, visible dans profilage profiledbt_source_
  - doublons : mêmes idS ou X Y, vus par tests, dont les résultats sont persistés dans un schema _dbt_test__audit par --store-failures
  - liens manquants : communes de Nouvelle Calédonie. Vu par test, marqué comme correct (par "where" spécifique)
  - analyses : corrigé bug de la macro pivot (PR en cours).
- test unitaire de non régression : de la normalization de source : contre son attendu _expected si --target test
- métamodèle - meta_indicators (v0) : par type et étape, depuis tag yaml et regex sur nom, group by, par SQL (plutôt que python ou js)
- TODO flacombe "vers table pour approche log ET pour voir (en simplifiant l'accès) l'évolution de l'usage dont la qualité, OK CKAN pour ça" => étudier dbt incremental et snapshot ; fal pour python après dbt TODO essayer !
- diverses veilles : --store-failures, dbt docs


## Ce que fait ce projet dbt :

- côté source :
 - exemples embarqués : _extract.csv (réel, à ne pas ouvrir sur github ?), __definition.csv (imaginaire)
  - TODO en cours de migration vers --target staging basé dessus, _source.yml dbt à fournir par leur projet dbt en dep ?
 contient des lignes choisies assez complètes du jeu OSM Geodatamine complet
 - renommage :
   - la sémantique est celle du standard supérieur : http://cnig.gouv.fr/?page_id=17477 . Nommage, mais aussi dictionnaires de valeurs qui manquent 
 - Lecture : intégré 1 170 002 lignes 100 mo en 30 min en 4000+ chunks par CKAN (vs 12-15 min de traitements dbt)
 - parsing :
   - géo de X Y WSG84 par PostGIS (AVEC srid 4326)
   - Q code INSEE commune : text sinon 01001 devient 1001 (conf dbt seed ou CKAN Datastore data dictionary)
   - NB. méthodo : sinon integer par dbt seed, ceux à parser depuis string suffixés par __s ou format (nombre, date), profiledbt_source_ pour voir pbs nettoyage (TODO rajouter compte)
 - nettoyage :
   - (a priori) pas car venant d'OSM ; sinon (détecter par échec de traitement,) traiter par règles SQL sur WHERE (ou bien avant par Excel ou Open Refine), après utiliser profilage pour confirmer la différence entre pas parsé et parsé et TODO tableau de bord de ça ; 
   - Q hauteur : pas nettoyé (contient "emental" (?), mais float et "" semblent passer), vu par AVG KO et dans profiledbt_source_ vs __s(ATTENTION le mapping n'est pas parfaitement 1 pour 1 ex. TypePhysique). Le profilage permet de le voir : Hauteur a 125 distincts mais Hauteur__s 127
     - => flacombe : TODO aucune donnée OSM n'est propre même si elles le semblent (sauf X Y produit par geodatamine de Point OSM), donc plutôt considérer champ texte et caster, TODO Q tester le type dans postgresql sinon retyper extraits ?? car risque que traitements pas stables dans le temps, DONC mettre dans exemple / _definition une ligne "pourrie" pour tester et bon type texte ; TODO comment automatiser la remontée d'erreur à la collectivité de type (ou autre) qu'elle garantit / de sa responsabilité ?
   - doublons : mêmes idS ou X Y, vus par tests (et) dans --store-failures
      - 128 poteaux en double avec même src_id ex. node/8817961325 node/3114733330 (et Id déduit), ont souvent des communes différentes => rajouter table relation n-n poteaux communes ?? ou la recalculer (reverse geocoding) ? (ou unique.where) => TODO c'est sans doute la jointure dans geodatamine et non osm qui donne ces doublons
      - 168 (sans doute que 40 en plus des précésents) X Y en double ex. POINT (6.0590188 48.3878446998437) POINT (5.8612464 48.2946089998666) et src_id / Id différents, et que rarement des communes différentes => Q campagnes différentes, rajouter leur table ? ou dédoublonner ?? (ou unique.where)
 - retypage d'exemple embarqué (CSV) et (TODO bug taille vs proxy) de vue sur fichier CSV importé dans CKAN,
 - réconciliation pour traduction de valeurs de dictionnaires (définis dans l_*.csv et liés dans l_*__<source>/mapping.csv),
   - NB. leur métamodèle : (l_)appuisaeriens (TODO Q unifier son nom ?) hérite de (l_)pointaccueil_ qui vient de GraceTHD mais sont tous des poteaux / supports
  - TODO LATER réconciliation approchée entre 2 sources DIFFERENTES ne partageant pas d'id :
   - appuiscommuns : < 5 m entre 2 poteaux signifie que c'est le même
   - méthodologie : pour commencer simple SQL DBT, à terme possibilité de mesure de proximité des termes / valeurs, avec recherche plein texte PostgreSQL voire intégration d'un moteur de déduplication complet, comme Duke (sur le moteur de recherche Lucene) https://github.com/larsga/Duke ou (en Python) https://github.com/dedupeio/dedupe , qui génère, entre entités entre 2 listes ou dans la même, des liens "les meilleurs possibles" à approuver / rajouter dans la base, mais seulement si la complexité d'intégration et de configuration par cas d'usage (champs) et collectivités (règles) en vaut la chandelle.
 - champs calculés à partir des précédents (uuidv5 reproductible un peu comme un hash, traduction de dictionnaire complexe, TODO LATER pour d'autres sources qui n'ont pas de commune, déduction par reverse geocoding),
 - tests (ou côté exploitation !?!) :
  - tests génériques par contraintes de schéma (_schema.yml) dont relationship avec source ET dictionnaire
    - Q résultats : total 1 170 002, et voir : réconciliation / doublons, enrichissement / liens manquants
  - test unitaire de non régression : d'une transformation (normalisation...) vs son __expected.csv validé qu'elle a produit auparavant. Sur le seul périmètre de __definition/union car les champs spécifiques à une seule source ne peuvent pas être prévus dans le projet cas d'usage mutualisé de base de source. Aidé de __expected_stg.sql qui fait les parsing requis non faits par dbt seed par défaut ni par configuration. Hors --target test, soit posé sur un model alors désactivé spécifique à cette fonctionnalité (_expected_stg.sql...), soit entre la transformation et elle-même plutôt que le _expected. TODO : simplifier (déplacer dbt seed config en _stg.sql ? dev equality(column_overrides=) ?...), sur exemple fabriqué / démarqué / __definition.csv
  - TODO de https://github.com/calogica/dbt-expectations (?)
  - outillage :
   - --store-failures stocke les lignes en erreur dans le schema _test__audit https://docs.getdbt.com/docs/building-a-dbt-project/tests#storing-test-failures
   - Q TODO (dans) dashboard, test.description, test.accepted_result_ids (ou par "where" spécifique ou https://docs.getdbt.com/reference/resource-configs/where générique), ? et plus, pour stocker et visualiser évolution et commentaires des problèmes
 - pas d'export depuis une base existante (Airbyte, à préférer à FDW, voir Synchronisation de la normalisation d'une base de données existante vers le datalake FDR à https://github.com/France-Data-Reseau/fdr_altereo_hpocollect )
 - outils : TODO BUG profilage

- côté exploitation :
 - NB. à la question "traitements plutôt avant ou après union" dbt répond "après" https://github.com/dbt-labs/dbt-core/issues/2716
 - unification : UNION par dbt_utils (et non explicite), uniquement des champs de la __definition.csv (permettant d'avoir d'autres champs spécifiques à la source dans sa normalisation)
  - enrichissement (ou côté source si spécifique, ou pour nettoyage ?) :
    - des communes ODS et de leurs populations, par left join et non join sinon les lignes sans valeur manquent)
    liens (communes NC TODO compte TODO Q donc joindre avant ? ou filtrer sur périmètre ??)
    - Q liens manquants : 7539 communes non dans le jeu ODS (Nouvelle-Calédonie qui collectivité sui generis, mais OK pour Wallis et Futuna qui collectivité d'outre mer). Solution : vu par test, marqué comme correct par "where" spécifique. Ou voire compte ou filtrer sur périmètre ? voire joindre dans source ??
      - => TODO OK pour verrouiller résultat test tel quel, mais pas stable, plutôt dire > '96000' ATTENTION à 2A Corse (MAIS quid du jour où il y en aura...) ; ET il faudrait filtrer sur les territoires très rapidement ex. X Y dans commune shape, en plus traitements beaucoup moins longs
      - select array_agg(distinct appco."appuiscommunssupp__fdrcommune__insee_id") from "datastore"."appuiscommuns"."appuiscommuns_supportaerien" appco
left join "datastore"."france-data-reseau"."georef-france-commune.csv" odscom on appco."appuiscommunssupp__fdrcommune__insee_id" = odscom."com_code" where odscom."com_code" is null
      - select
appco."appuiscommunssupp__fdrcommune__insee_id", count( appco."appuiscommunssupp__fdrcommune__insee_id") from "datastore"."appuiscommuns"."appuiscommuns_supportaerien" appco
left join "datastore"."france-data-reseau"."georef-france-commune.csv" odscom on appco."appuiscommunssupp__fdrcommune__insee_id" = odscom."com_code"
    where odscom."com_code" is null group by appco."appuiscommunssupp__fdrcommune__insee_id"
  - exploitation :
    - exemple de calcul d'indicateurs basiques : par commune et région par GROUP BY : count et par habitant, min / max / avg des nombres, valeurs distinctes des dictionnaires (par array_agg distinct) et compte pour chacun (par macro dbt pivot, patchée), TODO : aussi leur pourcentage (en adaptant la macro dbt), over time et des nouvelles lignes, TODO LATER test même nombre de lignes que source minus doublons
  - TODO Q filtrer sur périmètre quand ? à télécharger quoi ? découpés comment (quand par commune ex. _normalized ou pas ex. _indicators ou autre) et dans quelle org (si pas logique) ?
    - => flacombe : quoi et où publier et comment découpé et filtrer sur périmètre (et impact structure projet / étapes, et car matérialisation) => TODO la vue par périmètre géographique (territoire) mais pas la vue ; par défaut dans l'organisation qui la produit donc ici cas d'usage (mais recopie dans org source envisageable après)
   
- côté doc & métamodèle :
 - un nommage en amélioration (org_type_version et contenant éventuellement "sample" ou "extract", prefix__field, prefix__targetPrefix__targetField pour liens ; encore à améliorer y compris arbre des dossiers) ; TODO nommage générique pour faciliter l'adaptation pour une source ou avec projects vars "coll1" ( https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables ) ? et pour uuid namespace 
    - TODO Q formats de source (!= instance de ; la def / ex en est 1 !!) : quel nommage, macro-ter les .sql (dbt_utils.union() en est déjà et pas si simple à utiliser...) ? => OUI les normalizations / par source impl et facilite aussi expected (voire avec https://github.com/AgeOfLearning/dbt-unit-test ??)
    - TODO où mettre (prefix / schema / db) relations créées par : ex/def/expected test (rendre posssible dans ~dbt_mdutoo/dbt_ci), --store-failures, profilage
   - flacombe : TODO osmgeodatamine => osm, appuiscommuns => appcomm, enlever powsup
  - métamodèle :
    - sous forme de (doc et) tags (voire meta : type, pii...) dans _schema.yml (normalization, id / unique / uuid, sample / extract) et seeds CSV (type_definition, list, link ou relationship), et de "meta" (type, def? in(s), source?), et comment les générer et les exploiter (dbt docs generate puis serve et les entrer dans le champ de Recherche en cochant "Tags", ou target/catalog.json puis output textuel .md ou alimenter une IHM ex. React.js permettant de naviguer par tag voire schema / org(dataset) et type)
    - meta_indicators (v0) : tableau de bord d'avancement, d'adoption des étapes du traitement et des fonctionnalités du moteur. (models SQL) par type et étape, depuis tag yaml et regex sur nom, group by, par SQL (plutôt que python ou js) ; TODO : et par source, nb champs et liens, par def et leurs sources, transformation = normalization (1 in) ou enrichissement (plusieurs in mais 1 principal) ou x ; et impact nommage dbt models SQL
  - NB. veille doc dbt-style - au-delà limité : (column tags pas visibles dans dbt doc UI) description dans les .yml (model, column, sources), externalisables dans {% docs table_events %}{% enddocs %} dans .md et notamment __overview__, MAIS PAS dans .sql ni tests https://docs.getdbt.com/reference/dbt-jinja-functions/doc . Roadmap :
    - doc within .sql https://github.com/dbt-labs/dbt-core/issues/979
    - doc inheritance = same, renamed, transformed https://github.com/dbt-labs/dbt-core/issues/2995
 - TODO indicateurs de complétude et qualité, générique et métier. A l'aide de tout le précédent : profilage de source dont indicateurs de nettoyage voire de caractéristiques, tests génériques de caractéristiques et de non régression de transformation, indicateurs métier spécifiques ; et de tous évolution avec _over_time, envoyés comme ressources dans CKAN. TODO LATER alertes voire bloquage de processus ex. notion de "run" (avec guid comme --store-failures ?) publiable ou non (ex. par view SQL) relançable (dans Jenkins collaboratif ?)
   - TODO et au-delà, comment s'en servir pour suivre évolutions des données à correctement transformer et des indicateurs, et pour moissonnage / au fil de l'eau ?


**TODOs** :

- TODO données :
 - birdz et megalis :
  - TODO birdz : appuisaeriens, pas standard ; PDR_NUM source id ; plus type  VARCHAR 254 ; commençant par code INSEE commune OUI sinon quel est l'identifiant commune A TROUVER DEPUIS X Y DANS TOUS LES CAS !! DISTANCE < 20m ; pas de code_externe
  - TODO megalis : appuisaeriens, en GraceTHD v2 (pas v3 déjà envoyée) ; mais dans l'exemple aucun id / code ne référence celui dans birdz OUI 2 ENTREPRISES DIFFERENTES (gestionnaires de ce qui est supporté par l'appui et modélise des choses qu'il ne possède pas) il y a beaucoup de champs et surtout de codes, qui mériteraient un détail / doc et du mapping : pt_code est id source, pt_codeext référence métier / externe (reference OSM) du métier du gestionnaire du poteau = CodeExterne ;  id / code ORMB = propriétaire ex syndicat ; propriétés pt_ point technique GraceTHC vs nd_ noeud GraceTHD niveaux d'héritage GraceTHD dont on hérite
  - flacombe : doublons possibles entre toutes les sources
 - IRVE / "bornes de recharge" :
  - c'est bien beau de réutiliser des schémas existants, mais manque comment on les relie (ou pas), et les unités et valeurs sont-elles cohérentes ?
  - etalab irve : code_insee_commune ? siren_amenageur donc rajouter dataset organisations ? de quoi, parcelles personnes morales contenant siren et nom, ou sirene etalab https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/ ?
  - parcelles foncières : comment et à quoi les réconcilier ?? irve vers parcelle selon position ? quel usage ??
  - parking : insee = commune ou code ? num_siret : re donc rajouter dataset organisations ?

- TODO métamodèle : joli print auto .md de tout ce qu'il y a / sources et chaque étape (donc ont dossiers dédiés ?! pour chacune, puis pour les exploitations plus ou moins génériques ou spécifiques, et __definition.csv plus dicts.csv, voire déduire de profilage les types, formats et caracs ex. id) et plus à fond si formalisé, le mettre dans le README. A partir de : target/catalog.json dont les tags, macro on-run-end get_relations_by_pattern() ou jinja graph selectattr (voir union) ?
 - et / ou essayer de générer le métamodèle yaml ! partiellement au moins, depuis ça
 - et / ou le tableau matrice d'adoption des fonctionnalités et des étapes du processus de traitement par CU par les collectivités !
 - prototyper l'output de tout ça / un "ls" de ce projet, en .md dans ce README : (OU en tags et meta dans _schema.yml !!) :
  - modèle :
   - indicateurs :
   - (chaque) type : de *__definition.csv ou union modele/ ou all/
   - distinguer ceux qui sont listes statiques : de ex. l_*.csv SAUF l_*__<source>.csv qui sont liens de réconciliation / relations

- TODO refactoring et mutualisation :
 - extraction des éléments des sources vers autant de projets qui en dépendent ((fdr_)appuiscommuns_osm) ou plusieurs (fdr, et le partager avec les autres projets dbt)
 - peut-être des exemples en sous-projet integration_tests (vu dans https://github.com/ozwillo/dbt-profiler ). Essayer de faciliter sa prise en main : pas de mise en place préalable (base source) ou la faire une fois pour tout le monde...
 - org(_dataset)_source.yml : dans les projets dbt éponymes en dep, les tables / views SQL qu'il offre. Pourrait aussi inclure la création des vues sur les tables créée à l'import par CKAN avant l'automatisation ? voire l'automatisation par introspection SQL de la base CKAN (ou dans un projet dbt uniqué dédié) ??
 - union générique : par get_relations_by_pattern(schemas pas test, '%__casdusage_type.sql') OU jinja graph nodes selectattr filter https://docs.getdbt.com/reference/dbt-jinja-functions/graph https://oznetnerd.com/2017/04/18/jinja2-selectattr-filter/ (mais plus à fond pas encore d'actualité https://github.com/dbt-labs/dbt-core/issues/1212 https://github.com/dbt-labs/dbt-core/issues/2716 ) OU dbt core dev. 
 - tout côté exploitation : aussi dans projet dep ? permettrait de les avoir séparément pour cette source dans son schema (mais c'est peut-être mieux de les avoir dans la même table avec une colonne "source")
 - tout côté source : aussi dans projet dep ? permettrait d'avoir les exemples embarqués (mais sont déjà dans le projet source base), pourraient être référencés par _source.yml (ou remplacés si conf prop activée)
 - quels autres dbt models .sql dans projet dep : indicateurs de complétude et qualté etc. !
 - et ceux qu'on ne veut pas peuvent être désactivés, tous ou dans un dossier https://github.com/dbt-labs/dbt-core/issues/3043 ! (par ailleurs, ils peuvent être dans un autre schema, avoir des tags...)
 - Q comment mettre à jour la "mécanique" d'un projet de source (ou...) depuis son projet de base ? il faudrait qu'elle soit toute entière dans le projet dep ! et seuls les overrides et remplissage de CSV etc. dans le projet réel ?! MAIS pour l'instant pas d'overrides ( https://github.com/dbt-labs/dbt-core/issues/4157 , les seuls sont overrides sont de source https://docs.getdbt.com/reference/resource-properties/overrides ) donc simplement désactivés les models de dep à overrider et les copier et modifier dans le projet source.

- TODO nommage plus global et cohérent :
 - des fichiers : models (sample/src__fournisseur_source_typeunifie_version.sql), description de source (fournisseur_nom_source.yml), tests génériques de contrainte de schema ((fdr_?)casdusage_schema.yml), exploitation (perimetre__monexploitation.sql), profilage
 - dossiers : trouver une meillere organisation des fichiers
 - par étape du processus de traitement des données (source, normalized, unified, exploitation...) ?
 - (aidé par vars "source"/"coll1" ? et pour uuid namespace https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables)
 - supportant un seul projet dbt pour une source alimentant plusieurs cas d'usage ?
 - supportant au sein d'un seul projet dbt de la source ET de l'exploitation, par exemple par fournisseur (expert data steward) ou pour des tests ?
 - TODO et selon matérialisation / étapes / téléchargeables (voir plus bas)
 - TODO et selon patterns (donnant tags) permettant analytics metamodel seules dispos dans macros (et non tags)
 
- code python pour : alimenter la base datalake (depuis fichiers mis en ligne sur CKAN) ou préparer, exploiter en déhors de la base (ou avec fal https://github.com/fal-ai/fal )
- réconciliation et dictionnaires de valeurs : les rentrer plutôt par CKAN ?? (mais serait moins bien versionné)
- contraintes : relationship pour référence ou dictionnaire, https://github.com/calogica/dbt-expectations (pour le modèle des tests plus élargis de nom et type de colonnes, valeurs dans intervalle ou ordre ou regex ou liste au lieu de relationship, agrégats et proportions...)
- utilisation d'une source tableur mise dans le datalake par CKAN, devant être visbilisée par une view SQL
- enrichissement ex. depuis communes ODS, devant être visibilisée par une view SQL
- unification : dbt_utils.union_relations(), auto de toutes les relations ex. dans schemas des organisations CKAN dont un cas d'usage dépend hors tests ou selon un métamodèle

- TODO (et nommage aussi selon) quoi matérialiser : selon les besoins de performances (à tester !) : pour téléchargement & prévisualisation, donc des relations visibillisées dans CKAN :
 - logiquement sans doute pas normalization,
 - au moins (OU ???) unified (ex. osm_powersupport 14-65s)
 - et surtout indicators (ex. ) (qui pas performants en vue !?! et pas gros)
 - mais _enriched reste a priori un helper (de jointure, qu'on pourrait optimiser => TODO index FK) pour d'autre exploitations et trop lourd (beaucoup de colonnes), SAUF si c'est un vrai enrichissement ex. traitements / calculs métier lourds / externes comme DSL, qui prennent du temps et dont les résultats sont donc toujours mis sur la base datalake en asynchrône, et dans une table séparée i.e. dbt source pour que dbt ne risque pas de la détruire (TODO mais faire de l'incrémental / _over_time !!) et qu'il faut joindre après => TODO différencier ! _join != _union_computed (qui avant _join ou si en a besoin après ?!?)
 - une bonne raison de les envoyer en CSV à CKAN car ainsi il a le CSV donné à téléchargé ET les données en dur pour les recherches etc. du Datastore ?!
 - pour obtenir enrichissement (ex. 571-647s) ou indicateurs plus rapidement : SEULEMENT si beaucoup de différents
 - LATER index pour / si perfs ? https://docs.getdbt.com/reference/resource-configs/postgres-configs
 
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
- ouvrir les projets github, après élimination / démarquage de toutes données réelles
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
dbt test --store-failures # pour exécuter les tests (ET stocker les lignes en erreur dans le schema _test__audit) , d'une part génériques (de contraintes sur schémas configurées en .yml, notamment pour les modèles normalisés), et d'autre part spécifiques (requêtes dans des fichiers .sql sous tests/ renvoyant une ligne par erreur trouvée)
dbt docs generate
dbt docs serve # (--port 8001) sert la doc générée sur http://localhost:8000

# au-delà :
dbt run --target staging --select meta_indicators_by_type # un seul model SQL
dbt run test --store-failures # les lignes en erreurs des tests sont stockés (dans un schema _dbt_test__audit TODO mieux)

```

### DBT resources

Good primer tutorial https://www.kdnuggets.com/2021/07/dbt-data-transformation-tutorial.html