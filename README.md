# Projet dbt appuiscommuns et autres exemples France Data Réseau

Ce projet dbt (Data Build Tool) est consacré au cas d'usage Appuis Communs, dans le cadre de l'initiative France Data Réseau.

### OBSOLETE TODO :

- suite : source_or_test_ref() => ref() override, apcom_def_*_src hors test, test schema
- id découpler chaque src de std+kpi : pour garder des workflow simples :
  - chaque (type de) src est exécutée pour chaque source en changeant la configuration d'alias
- TODO on peut unifier AVANT translation, dans vue créée par macro !
- mécanisme de test de non régression : publié dans un schema et JEU _test tout public ! possibilité de débrayer certaines sources mais alors pas tout public ?? (alors _staging mêmes droits que normal)
- comment gérer des données imparfaites :
  - (fournir tableschema et y inciter)
  - OPTION pour chaque type de chaque type de source, définir des tests de contrainte de schéma ; les faire exécuter, remonter rapport et résultats
  - TODO que sur les données où les tests sont OK : analyser les résultats (json ?) pour en tirer le bon filtre de la liste des ressources prise de CKAN
  - normalisation qui par de et accepte des valeurs texte : OK sauf TODO seeds pour tests
- Lambert 93 : la plateforme stocke en 4326 WGS84, et peut fournir la projection Lambert 93 par conversion. Lambert 93 est une projection, donc imparfaite, et erronée en dehors de la métropole.
- dedup :
  - methodo : within source first otherwise too long
  - pb dedup : osm prio pourquoi ? parce que le uuid à garder est plutôt ceux des sources pour que les apcomeq les référence ! sinon les mettre à jour où multiple ids à la elastic "fusion" mais requiert array operators : https://stackoverflow.com/questions/4058731/can-postgresql-index-array-columns
- best practices : keep comments mostly in SQL (useful in compiled/ to debug, no problems with), DBT code that is
- debug : faire printer les infos de debug par dbt dans des commentaires SQL ! DOES NOT WORK IN MACROS
- methodo perfs : 1. écrire en SQL simple et efficace et tout "table" materialized, 2. si pas perf exécuter la première partie du "with" seulement (en commentant le reste) et si pas performant comprendre pourquoi avec explain (dans ex. psql ou dbeaver) et poser les index nécessaires ou changer le code, 3. tant que l'ensemble n'est pas performant faire pareil avec chaque partie suivante du with
- dedup custom source order (osm best) & perfs, then export as example & better example mechanism, then ocind pivotS and region, then macro
- exploitation :
  - unifier préfixes : type : apcom_supportaerien_etape (version de stockage (test) / étape du traitement) ; champ : apcomsup_champ (Id, IdSupportAerien ?), apcomsup_com_code ; dbt test bonnes pratiques de nommage !
  - segmenté : par matériau ; exploitant électrique ; occupant télécoms, technologie et cheminement
  - jointure d'un meilleur enriched avec reg_code/nom (extrait de cet utilisation !) pour décliner par commune et AODE (apcomoc__Gestionnaire ?) (en plus de total), en mutualisant avant group by
  - permettre pas que des exemples, donc aussi unification des apcom* autres que supportaerien
  - Projection des échéances de fin d'occupation, Suivi des typologies conventionnelles
- ::date => timestamp !! https://stackoverflow.com/questions/14113469/generating-time-series-between-two-dates-in-postgresql
- !!! pour arriver à livrer quelque chose, réaliser les exemples et partir d'eux !!
- id : _test pas schema mais models sur ce qui est à tester (tests davantage unitaires)
- pass data to dbt : dbt run --vars '{my_variable: my_value}' ou dbt run-operation --args '{my_variable: my_value}', ou générer un source.yml normé MAIS manque de métas sur les tables listées (à part leur nom normé) et pas possible d'y inclure la table CKAN datastore à visibiliser en vue SQL au préalable donc il faut un autre format en plus de toutes façons
- LATER show test results (state, report, bad data) : logs as json, target/run(_results)... https://stackoverflow.com/questions/61238395/can-the-results-of-dbt-test-be-converted-to-report
- better _computed macro ? or none ?
- OUI TODO def aussi pour chaque source car requis pour leur union si elles n'ont pas tous les champs OU sens inverse au-dessus de _translatedS macrotés ? ; LATER def de yaml, guidant aussi to/from_csv ?
- utilise Lambert 93 pour le calcul en m ? stocké aussi / seulement ?
- notion de order by field
- TODO Q how to share schema constraint tests ? & refactor (one yml per source folder ?)
- périmètres :
 - trouver et télécharger des fichiers geojson (ou quelconques), et les unifier en CSV ou import direct en base, avec index geo (requis ?)
 - s'en servir pour filtrer les données non de collectivités, notamment apcom OSM, et ainsi éviter des pbs de nettoyage et de performance
- uniformiser et universaliser les champs géo : x/y, geo_point/shape_4326, geo_point/shape_json ; enlever les champs d'entrée s'ils ne correspondent pas ex. ODS région point sans POINT
- re generate .csv seeds column types as text (sinon sujet à erreurs y compris de dbt ex.  3_2 => 32 int, facile à retyper ::int, permet nettoyage par règles), dans seeds.yml séparé avec tags déplacés dans models.yml (car erreur dbt found two schema.yml entries for the same resource), déjà surtyper dans tous les _translated, documenter la bonne pratique. En même temps que to/from ckan. TODO issue, idée *:text
- DONE (birdz...) uni* sources (.csv)_stg ; from_csv guidé par métamodèle
- DONE remplacer _extract par example dont vars proj yml, en même temps que dedup
- communes n-n, link macro générique, dedup <20m OK reste : TODO
- remonter dedup etc. en générique, en revalidant voire automatisant la conf perfs index, _translated.SQL with index
- object created or file created or meta from ckan

TODOs : descendre, union+ MAIS ET dedup12 (INDEX GEO REQUIS ET INCREMENTAL), réconciliation (nécessairement après normalization ; comme test relationship serait utile de mutualiser entre sources impls donc après union, MAIS si des champs source specific y aident il faut le faire avant en source specific, en étage précablé i.e macro séparée(s pour pouvoir ne remplacer que l'une ?!) ?), emental+, --target final, IRVE, OK _ot(w) ; vars, macroter pour sources ; birdz, refactoring... en fond : incremental (if incremental where updated >), snapshot (SCD2 : rajoute colonnes dbt_valid_from  dbt_valid_to MAIS pas trop en Open Data ; sinon publier par vue liant vers concretS, qui / selon indicateurs voire alertes, lançables depuis Jenkins ?)

TODO bouger :

idée : faciliter avec une partie des données (var en LIMIT)
https://github.com/dbt-labs/dbt-core/issues/401 https://github.com/dbt-labs/dbt-core/issues/1059
open data "pourri"
historiser le résultat (mais pas l'intéremédiaire), voire dans table unique en rajoutant la date ex. j ou semaine, ex. en post-hook
IRVE :
manque "pourquoi besoin de" ou "architecture fonctionnelle" qui fait le lien entre CU et architecture / notice technique (ex. exploratoire géo expert requiert WFS)

 - IRVE / "bornes de recharge" :
  - c'est bien beau de réutiliser des schémas existants, mais manque comment on les relie (ou pas) entre eux PAS, et les unités et valeurs sont-elles cohérentes ? => 
  - etalab irve : code_insee_commune CORRECT ? siren_amenageur donc rajouter dataset organisations NON ? de quoi, parcelles personnes morales contenant siren et nom, ou sirene etalab https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/ ? puissance_nominale en kW alors que dans modèle en kVA mais la comparaison sera faite en dehors de la plateforme pour commencer
  - parcelles foncières : comment et à quoi les réconcilier NON SIMPLE COUCHE SUPPLEMENTAIRE ?? irve vers parcelle selon geo ? quel usage ?? SI AVEC PARKING PAR GEO POLYGONE POUR SAVOIR SI ZONE PRIVEE OU PUBLIC mais pas pour la borne EXISTANTE, installent des nouvelles plutôt là où il
  - parking : insee = commune ou code PAS RECONCILIER QUE GROUP BY OU PAR GEO ? num_siret : re donc rajouter dataset organisations NON PAS LIER ?


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


## Retours sur les sources de données

20220222 OSM Geodatamine :
- modèle - manque la date de modification (requis pour incrémental et résoudre les doublons) : timestamp du node https://www.openstreetmap.org/api/0.6/changeset/49428808 ou created_by du changeset (groupe de modifications) https://www.openstreetmap.org/api/0.6/changeset/49428808
- modèle - types non respectés : TODO alphanum...
- CSV OK
- TODO copier doublons



## OBSOLETE Ce que fait ce projet dbt :

- côté source :
 - exemples embarqués : _extract.csv (réel, à ne pas ouvrir sur github ?), __definition.csv (imaginaire)
  - TODO en cours de migration vers --target staging basé dessus, _source.yml dbt à fournir par leur projet dbt en dep ?
 contient des lignes choisies assez complètes du jeu OSM Geodatamine complet
 - renommage :
   - la sémantique est celle du standard supérieur : http://cnig.gouv.fr/?page_id=17477 . Nommage, mais aussi dictionnaires de valeurs qui manquent 
 - Lecture : intégré 1 170 002 lignes 100 mo en 30 min en 4000+ chunks par CKAN (vs 12-15 min de traitements dbt)
   - métamodèle : la hauteur joue le rôle de slot de l'équipement, 4 types : support <- équipment <- occupation et les 2 par SuiviOccupation
 - parsing :
   - géo de X Y WSG84 par PostGIS (AVEC srid 4326)
   - Q code INSEE commune : text sinon 01001 devient 1001 (conf dbt seed ou CKAN Datastore data dictionary)
   - NB. méthodo : sinon integer par dbt seed, ceux à parser depuis string suffixés par __s ou format (nombre, date), profilage de source dans profiledbt_source_ pour voir pbs nettoyage (TODO rajouter au pourcentage le compte voire les plus / moins fréquents)
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
    - exemple de calcul d'indicateurs basiques : par commune et région par GROUP BY : count et par habitant, min / max / avg des nombres, valeurs distinctes des dictionnaires (par array_agg distinct) et compte pour chacun (par macro dbt pivot, patchée) ; TODO : aussi leur pourcentage (en adaptant la macro dbt), version over time (ot, à l'aide d'incremental) et que des nouvelles lignes / par périodes ex. semaine / mois, TODO LATER test même nombre de lignes que source minus doublons
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
  - NB. veille doc dbt-style - au-delà limité : (hélas column tags pas visibles dans dbt doc UI) description dans les .yml (model, column, sources), externalisables dans {% docs table_events %}{% enddocs %} dans .md et notamment __overview__ (et utilisable pour publier ex. profilage), MAIS PAS dans .sql ni tests https://docs.getdbt.com/reference/dbt-jinja-functions/doc . Roadmap :
    - doc within .sql https://github.com/dbt-labs/dbt-core/issues/979
    - doc inheritance = same, renamed, transformed https://github.com/dbt-labs/dbt-core/issues/2995
 - TODO indicateurs de complétude et qualité, générique et métier. A l'aide de tout le précédent : profilage de source dont indicateurs de nettoyage voire de caractéristiques, tests génériques de caractéristiques et de non régression de transformation, indicateurs métier spécifiques ; et de tous évolution avec _over_time, envoyés comme ressources dans CKAN OU / ET publiés en tableau .md dans docs (comme dbt-profiler). TODO LATER alertes voire bloquage de processus ex. notion de "run" (avec guid comme --store-failures ?) publiable ou non (ex. par view SQL) relançable (dans Jenkins collaboratif ?)
   - TODO et au-delà, comment s'en servir pour suivre évolutions des données à correctement transformer et des indicateurs, et pour moissonnage / au fil de l'eau ?


**TODOs** :

- TODO données :
 - birdz et megalis :
  - TODO birdz : appuisaeriens, pas standard ; PDR_NUM source id ; plus type  VARCHAR 254 ; commençant par code INSEE commune OUI sinon quel est l'identifiant commune A TROUVER DEPUIS X Y DANS TOUS LES CAS !! DISTANCE < 20m ; pas de code_externe
  - TODO megalis : appuisaeriens, en GraceTHD v2 (pas v3 déjà envoyée) ; mais dans l'exemple aucun id / code ne référence celui dans birdz OUI 2 ENTREPRISES DIFFERENTES (gestionnaires de ce qui est supporté par l'appui et modélise des choses qu'il ne possède pas) il y a beaucoup de champs et surtout de codes, qui mériteraient un détail / doc et du mapping : pt_code est id source, pt_codeext référence métier / externe (reference OSM) du métier du gestionnaire du poteau = CodeExterne ;  id / code ORMB = propriétaire ex syndicat ; propriétés pt_ point technique GraceTHC vs nd_ noeud GraceTHD niveaux d'héritage GraceTHD dont on hérite
  - flacombe : doublons possibles entre toutes les sources

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

 
- code python pour : alimenter la base datalake (depuis fichiers mis en ligne sur CKAN) ou préparer, exploiter en déhors de la base (ou avec fal https://github.com/fal-ai/fal )
- réconciliation et dictionnaires de valeurs : les rentrer plutôt par CKAN ?? (mais serait moins bien versionné)
- contraintes : relationship pour référence ou dictionnaire, https://github.com/calogica/dbt-expectations (pour le modèle des tests plus élargis de nom et type de colonnes, valeurs dans intervalle ou ordre ou regex ou liste au lieu de relationship, agrégats et proportions...)
- utilisation d'une source tableur mise dans le datalake par CKAN, devant être visbilisée par une view SQL
- enrichissement ex. depuis communes ODS, devant être visibilisée par une view SQL
- unification : dbt_utils.union_relations(), auto de toutes les relations ex. dans schemas des organisations CKAN dont un cas d'usage dépend hors tests ou selon un métamodèle

- quoi matérialiser (et nommage aussi selon) : selon les besoins et les transformations à optimiser
 - selon les besoins : pour téléchargement & prévisualisation, donc des relations visibillisées dans CKAN :
  - logiquement sans doute pas normalization (les transformations ligne à ligne peuvent rester une vue), sauf si besoin d'index
  - au moins (OU ???) unified (ex. osm_powersupport 14-65s)
  - et surtout indicators (ex. ) (qui pas performants en vue !?! et pas gros)
  - mais _enriched reste a priori un helper (de jointure, qu'on pourrait optimiser => TODO index FK) pour d'autre exploitations et trop lourd (beaucoup de colonnes), SAUF si c'est un vrai enrichissement ex. traitements / calculs métier lourds / externes comme DSL, qui prennent du temps et dont les résultats sont donc toujours mis sur la base datalake en asynchrône, et dans une table séparée i.e. dbt source pour que dbt ne risque pas de la détruire (TODO mais faire de l'incrémental / _over_time !!) et qu'il faut joindre après => TODO différencier ! _join != _union_computed (qui avant _join ou si en a besoin après ?!?)
  - & une bonne raison de les envoyer en CSV à CKAN car ainsi il a le CSV donné à téléchargé ET les données en dur pour les recherches etc. du Datastore ?!
 - pour la performances des transformations - méthodologie :
  - définir tous les models dbt en table, en inliné dans le model : {{ config(materialized="table"
  - exécuter (dbt run) ; si trop lent :
   - copier le compilé (sans lignes vides : ) dans dbeaver après EXPLAIN,
   - et rajouter les index manquants dans la précédente config inlinée (voire rajouter les index a priori : ceux sur lesquels sont faits jointures, group by...), par exemple : . Voir https://docs.getdbt.com/reference/resource-configs/postgres-configs
  - de nouveau exécuter, et tant que c'est toujours trop lent, EXPLAIN et rajouter index
  - enfin, repasser les matérlisations table non indispensables en view
 - pour obtenir enrichissement (ex. 571-647s) ou indicateurs plus rapidement : SEULEMENT si beaucoup de différents
 - LATER index pour / si perfs ? 
 
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


### Nommage

- FINAL :
    - apcom_birdz_type_src est la dbt source qui unit (hors dbt donc) toutes les données source au format birdz
    - apcom_def_type_src est la dbt source qui unit (hors dbt donc) toutes les données source au format natif
- type
- partitionnement (qui peut être type de source voire source avant normalisation, divers enrichissement pour divers usages après)
- (date : oui est une "version" mais pas un partitionnement, en général elle est DANS la donnée, à moins d'être un snapshot figé, car si pas figé est une branche et donc pas vraiement une date)
- workflow, étape de, majeure : peuvent en être des indicateurs / guides. Ils sont :
    - "source" (décliné par source : megalis... TODO native, CSV). Les éléments discriminants sont le type fourni, et si nécessaire le traitement appliqué, voire le type source (SI un type normalisé provient de plusieurs types sources, qui sont alors peut-être autant de sous-types de sources).
    - "normalisation" sur sa partie définition, unification et déduplication. Les éléments discriminants sont le type fourni, puis l'étape appliquée.
    - "exploitation" / usage (indicateurs / kpi, mais sans doute pas la version CSV, geopackage, geoserver). Les discriminants sont :
        - le concept support de la métrique qui est souvent un type,
        - la métrique (linéaire de canalisation, poteau électrique ou technologie d'équipement...) MAIS le plus souvent cette relation concept suffit à fournir beaucoup / toutes les métriques (par des group by différents en dataviz ex. superset),
        - puis si nécessaire les dimensions (territoire reg/dep/commune qui est une hiérarchie multiple avec AODE, éventuellement métier ex. sur/sous-types de matériau ; mais tout cela est de l'enrichissement) et leurs grains offerts, y compris temporelle (là il peut y avoir des choses à faire : générer des jours ou avoir préparé un historique en SCD2 / DBT snapshot)    - Les enrichissements sont des suppléments de "normalisation" ou (que/et) des requis de "exploitation".
- Ils peuvent ainsi être classés dans des dossiers : apcom (normalisation, qui est aussi le dossier du projet), (apcom/)(src ou source/)megalis..., (apcom/)(exploitation/)kpi(/kpi1 ex. d'occupation). Cette classification peut être utilement reprise en préfixe de relation SQL / model DBT :
    - apcom_(src_)osm_supportaerien(_translated,deduped), apcom_std_supportaerien(_unified,deduped), apcom_(use_)kpi_suivioccupation_day

OBSOLETE :
- TODO nommage plus global et cohérent :
- des fichiers : models (sample/src__fournisseur_source_typeunifie_version.sql), description de source (fournisseur_nom_source.yml), tests génériques de contrainte de schema ((fdr_?)casdusage_schema.yml), exploitation (perimetre__monexploitation.sql), profilage
- dossiers : trouver une meillere organisation des fichiers
- par étape du processus de traitement des données (source, normalized, unified, exploitation...) ?
- (aidé par vars "source"/"coll1" ? et pour uuid namespace https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-variables)
- supportant un seul projet dbt pour une source alimentant plusieurs cas d'usage ?
- supportant au sein d'un seul projet dbt de la source ET de l'exploitation, par exemple par fournisseur (expert data steward) ou pour des tests ?
- TODO et selon matérialisation / étapes / téléchargeables (voir plus bas)
- TODO et selon patterns (donnant tags) permettant analytics metamodel seules dispos dans macros (et non tags)

### FAQ :

* column "appuiscommunssupp__Gestionnaire__None" does not exist
14:24:34    HINT:  There is a column named "appuiscommunssupp__Gestionnaire__None" in table "appuiscommuns__supportaerien_indicators_region_ot", but it cannot be referenced from this part of the query.
14:24:34    compiled SQL at target/run/fdr_appuiscommuns/models/exploitation/appuiscommuns__supportaerien_indicators_region_ot.sql
=> la structure du flux incrémental a changé par rapport à ce qu'il avait précédemment entré dans sa table historisée _ot, supprimer cette dernière (ou la migrer si on souhaite en garder les anciennes données)

* Unable to do partial parsing because a project config has changed
=> rm target/partial_parse.msgpack https://stackoverflow.com/questions/68439855/in-dbt-when-i-add-a-well-formatted-yml-file-to-my-project-i-stop-being-able-t

Gotchas - DBT :
- See test failures : store them in the database : dbt test --store-failures https://docs.getdbt.com/docs/building-a-dbt-project/tests https://github.com/dbt-labs/dbt-core/issues/2593 https://github.com/dbt-labs/dbt-core/issues/903
- index : https://docs.getdbt.com/reference/resource-configs/postgres-configs
- introspect compiled model : https://docs.getdbt.com/reference/dbt-jinja-functions/graph
- embed yaml conf in .sql : https://docs.getdbt.com/reference/dbt-jinja-functions/fromyaml
- dbt reuse : macros, packages (get executed first like they would be on their own including .sql files, but can pass different variables through root dbt_project.yml (?) ; TODO Q subpackages ?) https://www.fivetran.com/blog/how-to-re-use-dbt-guiding-rapid-mds-deployments
- run_query() must be conditioned by execute else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute

Gotchas - Jinja2 :
- doc https://jinja.palletsprojects.com/en/3.0.x/templates
- map() filter returns string "<generator object do_map at 0x10bd730>" => add |list https://github.com/pallets/jinja/issues/288
- change the value of a variable (esp. in a loop to find something) : not possible (and not in the spirit). But if really required, use a dict:  https://stackoverflow.com/questions/9486393/jinja2-change-the-value-of-a-variable-inside-a-loop
- macros accept other macros as arguments https://stackoverflow.com/questions/69079158/can-dbt-macros-accept-other-macros-as-arguments
- error The object ("{obj}") was used as a dictionary. This capability has been removed from objects of this type. => string utilisée en tant que list

Gotchas - DBeaver :
- a big query (with WITH statement...) throws error : DBeaver uses ";" character AND empty lines as statements separator, so remove these first https://dbeaver.io/forum/viewtopic.php?f=2&t=1687
- sometimes, 

Gotchas - PostgreSQL :
- HINT:  No function matches the given name and argument types. => add explicit type casts to the arguments
- FAQ postgres blocks & logs says WARNING:  there is already a transaction in progress => try restarting DBeaver (see above), or else terminate all running queries :
SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE state = 'active' and pid <> pg_backend_pid();
- drop all tables in a schema :
```sql
DO $$
DECLARE
row record;
BEGIN
FOR row IN SELECT * FROM pg_tables WHERE schemaname = 'eaupotable'
LOOP
EXECUTE 'DROP TABLE eaupotable.' || quote_ident(row.tablename) || ' CASCADE';
END LOOP;
END;
$$;
```

Gotchas - FAL :
- either setup one fal script ex. publish.py on EACH model, or a single script declared at <schema>.yml top level
- fal run --all (or --select ...) ; else runs no script
- access dbt context variables beyond what fal provides (ex. target, graph...) :
    - either using execute_sql() : schema = execute_sql("select '{{ target.schema }}'").values[0][0]
    - or using fal as a python lib from a .py file
- run dbt macro : either using execute_sql() (or in dbt model or hook such as on-run-start/end and run it all using fal flow run)
- peut désormais exécuter aussi avant (--)before, mais pas encore de macro dbt
- passing arguments :
  - local and public : as dbt "metas" at the location of the script declaration accessed by
  - BUT global or secret : as OS env vars accessed through python ( https://github.com/fal-ai/fal/tree/main/examples/slack-example ; rather than env_var("DBT_ENV_SECRET_...") which is only accessible in profiles/packages.yml see https://docs.getdbt.com/reference/dbt-jinja-functions/env_var https://github.com/dbt-labs/dbt-core/issues/2514 )
- airbyte :)


DBT 101 :
- snapshots : SCD2 that can be updated / run separately
- exposures : define outside uses in YAML, to publish doc and to run them separately
- metrics : defined in YAML, notably : time_grains=[day, week, month], dimensions=[plan, country], filters ; pour doc, MAIS ne produisent pas (de relation / model / macro) en elles-mêmes


## Install, build & run

**IMPORTANT** après dbt deps, remplacer le contenu de dbt_packages/dbt_profiler par celui de https://github.com/ozwillo/dbt-profiler (attention au "_", ne sera plus nécessaire quand une nouvelle version aura été publiée incluant https://github.com/data-mie/dbt-profiler/pull/38 )

### Install : DBT (1.0), fal, ckanapi

comme à https://docs.getdbt.com/dbt-cli/install/pip :
(mais sur Mac OSX voir https://docs.getdbt.com/dbt-cli/install/homebrew )

```shell
sudo apt-get install git libpq-dev python-dev python3-pipsudo apt-get remove python-cffisudo pip install --upgrade cffipip install cryptography~=3.4

sudo apt install python3.8-venv
python3 -m venv dbt-env
source dbt-env/bin/activate
pip install --upgrade pip wheel setuptools
pip install dbt-postgres

pip install fal
pip install ckanapi
pip install requests

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

# debug :
vi logs/dbt.log

```

### DBT resources

Good primer tutorial https://www.kdnuggets.com/2021/07/dbt-data-transformation-tutorial.html
