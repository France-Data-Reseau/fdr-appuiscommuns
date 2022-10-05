{#
Normalisation vers le modèle de données du cas d'usage "appuiscommuns" des données de type supportaerien
de la source de type "gthdv2"
Partie spécifique à la source

Inclus computed (spécifique), ainsi qu'add_generic_fields()

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la __definition) ?
#}

{% macro gthdv2__apcom_supportaerien(parsed_source_relation, src_priority=None, typed_names=False) %}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'apcom_gthdv2_deftest' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set fdr_namespace = 'supportaerien.' + var('fdr_namespace') %} -- ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'gthdv2' %} -- ?
{% set prefix = 'apcomsup' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '_' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '_' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

with import_parsed as (

    select * from {{ parsed_source_relation }}
    {% if var('limit', 0) > 0 %}
    LIMIT {{ var('limit') }}
    {% endif %}

{#
rename and generic parsing is rather done
- in specific _from_csv
- in generic from_csv (called by fdr_source_union), which is guided by the previous one
#}

), typed_names_renamed as ( -- TODO or in from_csv() after building specific mappping ??

select
{% if typed_names %}
        *, -- TODO if test to debug / find conversion root error, else source('france-data-reseau', 'fdr_def_generic_fields_definition')
        --geometry,

        -- pt :
        -- Table non géographique décrivant les points techniques de l'infrastructure d'accueil. Cette table peut être rendue géographique (Points) en localisant le noeud associé à un point technique au moyen de l'implémentation de la relation pt-localise-par
        -- TODO TODO Q geometry, missing !!!
        "pt_code,C,254"::text as "pt_code", -- NOT NULL source own id PTMB0100702556
        "pt_codeext,C,254"::text as "pt_codeext", -- ERDF632/29024 Code chez un tiers ou dans une autre base de données
        "pt_etiquet,C,254"::text as "pt_etiquet", -- TODO Q OU MERGE CodeExterne ? Etiquette sur le terrain
        -- pt_nd_code : voir nd_code NON VIDE
        "pt_ad_code,C,254"::text as "pt_ad_code", -- FK OBSOLETE (Les attributs d'adressage postal et cadastral table via t_ptech_patch202) : Identifiant unique de l'adresse du point technique (pour les points techniques qui peuvent être associés à une adresse précise).
        "pt_gest_do,C,20"::text as "pt_gest_do", -- FK Gestionnaire du domaine sur lequel est situé le point technique
        "pt_prop_do,C,20"::text as "pt_prop_do", -- FK Propriétaire du domaine sur lequel est situé le point technique
        "pt_prop,C,20"::text as "pt_prop", -- ORMB0000000003 FK Propriétaire du point technique = AODE ! mais comment SIREN, table statique séparée ? PEUT être différent un mois plus tard !!
        "pt_gest,C,20"::text as "pt_gest", -- FK Gestionnaire du point technique
        "pt_user,C,20"::text as "pt_user", -- FK Utilisateur du point technique
        "pt_proptyp,C,3"::text as "pt_proptyp", -- LOC FK Type de propriété
        "pt_statut,C,3"::text as "pt_statut", -- REC FK NOT NULL Phase d'avancement
        "pt_etat,C,3"::text as "pt_etat", -- OK FK Etat du point technique
        "pt_dateins,D"::text as "pt_dateins", -- Date d'installation
        "pt_datemes,D"::text as "pt_datemes", -- Date de mise en service
        "pt_avct,C,1"::text as "pt_avct", -- E FK Avancement du projet
        "pt_typephy,C,1"::text as "pt_typephy", -- A FK NOT NULL Type de point technique
        "pt_typelog,C,1"::text as "pt_typelog", -- R FK NOT NULL Usage du point technique
        "pt_rf_code,C,254"::text as "pt_rf_code", -- FK NOT NULL Référence
        "pt_nature,C,20"::text as "pt_nature", -- PIND FK Nature du point technique
        "pt_secu,N,1,0"::text as "pt_secu", -- 0 Le point technique est-il équipé d'un système de verrouillage, ou de tout autre système permettant d'en sécuriser l'accès ?
        "pt_occp,C,10"::text as "pt_occp", -- FK Occupation
        "pt_a_dan,N,24,15"::text as "pt_a_dan", -- 0 Effort disponible après pose (exprimé en daN – décanewtons)
        "pt_a_dtetu,D"::text as "pt_a_dtetu", -- Date de l'étude de charge
        "pt_a_struc,C,100"::text as "pt_a_struc", -- Simple, Moisé, Haubané, Couple, ...
        "pt_a_haut,N,6,2"::text as "pt_a_haut" , -- 0 Hauteur en mètre entre le sol et la base de l'infrastructure (réseau en façade ou aérien)
        "pt_a_passa,N,1,0"::text as "pt_a_passa", -- 0 0 pour passage de câbles uniquement
        "pt_a_strat,N,1,0"::text as "pt_a_strat", -- 0 Appui stratégique. Notion Orange. disponible dans les PIT (STRATEGIQU). Extensible à d'autres types de réseaux.
        "pt_rotatio,N,6,2"::text as "Azimut", -- 0 Angle du grand axe du point technique en degrés dans le sens rétrograde (sens des aiguilles d'une montre) à partir du Nord
        "pt_detec,N,1,0"::text as "pt_detec", -- 0 Présence d'un boitier pour un fil de détection
        "pt_comment,C,254"::text as "pt_comment", -- Commentaire
        "pt_creadat,C,24"::text as "pt_creadat", -- 2021/03/09 00:00:00.000 Date de création de l'objet dans le S.I.
        "pt_majdate,C,24"::text as "pt_majdate", -- Dernière date de mise à jour de l'objet dans le S.I.
        "pt_majsrc,C,254"::text as "pt_majsrc", -- Source utilisée pour la mise à jour
        "pt_abddate,D"::text as "pt_abddate", -- Date d'abandon (fin de validité) de l'objet dans le S.I.
        "pt_abdsrc,C,254"::text as "pt_abdsrc", -- Motif de l'abandon de l'objet

        -- nd :
        -- Table géographique contenant les noeuds de l'infrastructure d'accueil
        "nd_code,C,254" as "nd_code", -- NDMB0100702573
        "nd_codeext,C,254"::text as "nd_codeext", -- ERDF632/29024 Code chez un tiers ou dans une autre base de données
        "nd_nom,C,254"::text as "nd_nom", -- NOT LATEST STANDARD (t_ptech_patch202)
        "nd_coderat,C,254"::text as "nd_coderat", -- NOT LATEST STANDARD
        "nd_r1_code,C,100"::text as "nd_r1_code", -- T1-29 NOT LATEST STANDARD
        "nd_r2_code,C,100"::text as "nd_r2_code", -- 1007 NOT LATEST STANDARD
        "nd_r3_code,C,100"::text as "nd_r3_code", -- NOT LATEST STANDARD
        "nd_r4_code,C,100"::text as "nd_r4_code", -- NOT LATEST STANDARD
        "nd_voie,C,254"::text as "nd_voie", -- 29024/CARHAIX-PLOUGUER/RUE SÉVIGNÉ//
        "nd_type,C,2"::text as "nd_type", -- PT FK Type du nœud (se déduit de la relation d'héritage
        "nd_type_ep,C,3"::text as "nd_type_ep", -- OPT FK Liste des technologies présentes (1 à 5 occurrences)
        "nd_comment,C,254"::text as "nd_comment", -- Commentaires
        "nd_dtclass,C,2"::text as "nd_dtclass", -- CLASSEPRECISION PLUTOT DE LA LONGUEUR FK Classe de précision au sens du décret DT-DICT
        "nd_geolqlt,N,7,2"::text as "nd_geolqlt", -- 0 Précision du positionnement de l'objet, estimée en mètres. La précision doit être déduite du mode d'implantation et du support d'implantation, en tenant compte selon les cas du cumul des imprécisions : des levés ou du fond de plan (utiliser dans ce cas la classe de précision planimétrique au sens de l'arrêté du 16 septembre 2003), de l'outil de détection, des cotations, de l'éventuel report 'à main levée', etc.
        "nd_geolmod,C,4"::text as "nd_geolmod", -- FK Mode d'implantation de l'objet
        "nd_geolsrc,C,254"::text as "nd_geolsrc", -- Source de la géolocalisation pour préciser la source si nécessaire
        "nd_creadat,C,24"::text as "nd_creadat", -- 2021/03/09 00:00:00.000 Date de création de l'objet dans le S.I.
        "nd_majdate,C,24"::text as "nd_majdate", -- Dernière date de mise à jour de l'objet dans le S.I.
        "nd_majsrc,C,254"::text as "nd_majsrc", -- Source utilisée pour la mise à jour
        "nd_abddate,D"::text as "nd_abddate", -- Date d'abandon (fin de validité) de l'objet dans le S.I.
        "nd_abdsrc,C,254"::text as "nd_abdsrc", -- Motif de l'abandon de l'objet

        "nd_voie,C,254" as "nd_voie"
{% else %}
*
{% endif %}
from import_parsed

), renamed as (

    -- standard gthdv2 patch .2 :
    -- http://cnig.gouv.fr/wp-content/uploads/2019/07/COVADIS_standard_ANT_v2-0-2_GraceTHD_rc2.pdf
    -- et tables SQL : https://github.com/GraceTHD-community/GraceTHD-MCD/blob/master/sql_postgis/gracethd_30_tables.sql
    -- et valeurs : https://github.com/GraceTHD-community/GraceTHD-MCD/blob/master/sql_postgis/gracethd_20_insert.sql

    -- ne créé que des poteaux, mais tous occupés (mais sans dire par quelles occupations)

    -- sont de type "table géographique" et ont donc sans doute un champ geometry en plus
    -- de ceux listés dans le standard :
    -- t_cableline/cheminement/noeud/adresse/z*/empreinte

    -- TODO Q analyse de l'exemple mégalis :
    -- PAS DE GEOMETRY
    -- empile point technique (ptech/pt) et noeud (nd) : code != mais codeext = ?!
    -- Structure est contraint mais pt_a_struc non !!'
    -- Fabricant : de prop(_do) ??
    -- pt_abddate est la seule des 6 dates d'audit à être notée de type D et non C
    -- hauteur(s) !!
    -- BandeauVert Date de l'étude de charge ??
    select
        *, -- TODO if test to debug / find conversion root error, else source('france-data-reseau', 'fdr_def_generic_fields_definition')
        --geometry,

        -- pt :
        -- Table non géographique décrivant les points techniques de l'infrastructure d'accueil. Cette table peut être rendue géographique (Points) en localisant le noeud associé à un point technique au moyen de l'implémentation de la relation pt-localise-par
        -- TODO TODO Q geometry, missing !!!
        "pt_code"::text as "{{ fieldPrefix }}src_id", -- NOT NULL source own id PTMB0100702556
        "pt_code"::text as "{{ fieldPrefix }}IdSupportAerien", -- NOT NULL source own id PTMB0100702556
        "pt_codeext"::text as "{{ fieldPrefix }}CodeExterne", -- ERDF632/29024 Code chez un tiers ou dans une autre base de données
        "pt_etiquet"::text as "{{ sourceFieldPrefix }}pt_etiquet", -- TODO Q OU MERGE CodeExterne ? Etiquette sur le terrain
        -- pt_nd_code : voir nd_code NON VIDE
        "pt_ad_code"::text as "{{ sourceFieldPrefix }}pt_ad_code", -- FK OBSOLETE (Les attributs d'adressage postal et cadastral table via t_ptech_patch202) : Identifiant unique de l'adresse du point technique (pour les points techniques qui peuvent être associés à une adresse précise).
        "pt_gest_do"::text as "{{ sourceFieldPrefix }}pt_gest_do", -- FK Gestionnaire du domaine sur lequel est situé le point technique
        "pt_prop_do"::text as "{{ sourceFieldPrefix }}pt_prop_do", -- FK Propriétaire du domaine sur lequel est situé le point technique
        "pt_prop"::text as "{{ fieldPrefix }}Proprietaire", -- ORMB0000000003 FK Propriétaire du point technique = AODE ! mais comment SIREN, table statique séparée ? PEUT être différent un mois plus tard !!
        "pt_gest"::text as "{{ fieldPrefix }}Gestionnaire", -- FK Gestionnaire du point technique
        "pt_user"::text as "{{ sourceFieldPrefix }}pt_user", -- FK Utilisateur du point technique
        "pt_proptyp"::text as "{{ sourceFieldPrefix }}pt_proptyp", -- LOC FK Type de propriété
        "pt_statut"::text as "{{ sourceFieldPrefix }}pt_statut", -- REC FK NOT NULL Phase d'avancement
        "pt_etat"::text as "{{ sourceFieldPrefix }}pt_etat", -- OK FK Etat du point technique
        "pt_dateins"::text as "{{ sourceFieldPrefix }}DateConstruction__s", -- Date d'installation
        "pt_datemes"::text as "{{ sourceFieldPrefix }}pt_datemes__s", -- Date de mise en service
        "pt_avct"::text as "{{ sourceFieldPrefix }}pt_avct", -- E FK Avancement du projet
        "pt_typephy"::text as "{{ sourceFieldPrefix }}pt_typephy", -- A FK NOT NULL Type de point technique
        "pt_typelog"::text as "{{ sourceFieldPrefix }}pt_typelog", -- R FK NOT NULL Usage du point technique
        "pt_rf_code"::text as "{{ sourceFieldPrefix }}pt_rf_code", -- FK NOT NULL Référence
        "pt_nature"::text as "{{ sourceFieldPrefix }}pt_nature", -- PIND FK Nature du point technique
        "pt_secu"::text as "{{ sourceFieldPrefix }}pt_secu__s", -- 0 Le point technique est-il équipé d'un système de verrouillage, ou de tout autre système permettant d'en sécuriser l'accès ?
        "pt_occp"::text as "{{ sourceFieldPrefix }}pt_occp", -- FK Occupation
        "pt_a_dan"::text as "{{ sourceFieldPrefix }}pt_a_dan__s", -- 0 Effort disponible après pose (exprimé en daN – décanewtons)
        "pt_a_dtetu"::text as "{{ sourceFieldPrefix }}pt_a_dtetu__s", -- Date de l'étude de charge
        "pt_a_struc"::text as "{{ sourceFieldPrefix }}pt_a_struc", -- Simple, Moisé, Haubané, Couple, ...
        "pt_a_haut"::text as "{{ sourceFieldPrefix }}HauteurAppui__s" , -- 0 Hauteur en mètre entre le sol et la base de l'infrastructure (réseau en façade ou aérien)
        "pt_a_passa"::text as "{{ sourceFieldPrefix }}pt_a_passa__s", -- 0 0 pour passage de câbles uniquement
        "pt_a_strat"::text as "{{ sourceFieldPrefix }}pt_a_strat__s", -- 0 Appui stratégique. Notion Orange. disponible dans les PIT (STRATEGIQU). Extensible à d'autres types de réseaux.
        "pt_rotatio"::text as "{{ sourceFieldPrefix }}Azimut__s", -- 0 Angle du grand axe du point technique en degrés dans le sens rétrograde (sens des aiguilles d'une montre) à partir du Nord
        "pt_detec"::text as "{{ sourceFieldPrefix }}pt_detec__s", -- 0 Présence d'un boitier pour un fil de détection
        "pt_comment"::text as "{{ sourceFieldPrefix }}pt_comment", -- Commentaire
        "pt_creadat"::text as "{{ sourceFieldPrefix }}pt_creadat__s", -- 2021/03/09 00:00:00.000 Date de création de l'objet dans le S.I.
        "pt_majdate"::text as "{{ sourceFieldPrefix }}pt_majdate__s", -- Dernière date de mise à jour de l'objet dans le S.I.
        "pt_majsrc"::text as "{{ sourceFieldPrefix }}pt_majsrc", -- Source utilisée pour la mise à jour
        "pt_abddate"::text as "{{ sourceFieldPrefix }}pt_abddate__s", -- Date d'abandon (fin de validité) de l'objet dans le S.I.
        "pt_abdsrc"::text as "{{ sourceFieldPrefix }}pt_abdsrc", -- Motif de l'abandon de l'objet

        -- nd :
        -- Table géographique contenant les noeuds de l'infrastructure d'accueil
        case when "nd_code" is not null then "nd_code"::text else "pt_nd_code"::text end as "{{ sourceFieldPrefix }}nd_code", -- NDMB0100702573
        "nd_codeext"::text as "{{ sourceFieldPrefix }}nd_codeext", -- ERDF632/29024 Code chez un tiers ou dans une autre base de données
        "nd_nom"::text as "{{ sourceFieldPrefix }}nd_nom", -- NOT LATEST STANDARD (t_ptech_patch202)
        "nd_coderat"::text as "{{ sourceFieldPrefix }}nd_coderat", -- NOT LATEST STANDARD
        "nd_r1_code"::text as "{{ sourceFieldPrefix }}nd_r1_code", -- T1-29 NOT LATEST STANDARD
        "nd_r2_code"::text as "{{ sourceFieldPrefix }}nd_r2_code", -- 1007 NOT LATEST STANDARD
        "nd_r3_code"::text as "{{ sourceFieldPrefix }}nd_r3_code", -- NOT LATEST STANDARD
        "nd_r4_code"::text as "{{ sourceFieldPrefix }}nd_r4_code", -- NOT LATEST STANDARD
        "nd_voie"::text as "{{ sourceFieldPrefix }}nd_voie", -- 29024/CARHAIX-PLOUGUER/RUE SÉVIGNÉ//
        "nd_type"::text as "{{ sourceFieldPrefix }}nd_type", -- PT FK Type du nœud (se déduit de la relation d'héritage
        "nd_type_ep"::text as "{{ sourceFieldPrefix }}nd_type_ep", -- OPT FK Liste des technologies présentes (1 à 5 occurrences)
        "nd_comment"::text as "{{ sourceFieldPrefix }}nd_comment", -- Commentaires
        "nd_dtclass"::text as "{{ sourceFieldPrefix }}nd_dtclass", -- CLASSEPRECISION PLUTOT DE LA LONGUEUR FK Classe de précision au sens du décret DT-DICT
        "nd_geolqlt"::text as "{{ sourceFieldPrefix }}nd_geolqlt__s", -- 0 Précision du positionnement de l'objet, estimée en mètres. La précision doit être déduite du mode d'implantation et du support d'implantation, en tenant compte selon les cas du cumul des imprécisions : des levés ou du fond de plan (utiliser dans ce cas la classe de précision planimétrique au sens de l'arrêté du 16 septembre 2003), de l'outil de détection, des cotations, de l'éventuel report 'à main levée', etc.
        "nd_geolmod"::text as "{{ sourceFieldPrefix }}nd_geolmod", -- FK Mode d'implantation de l'objet
        "nd_geolsrc"::text as "{{ sourceFieldPrefix }}nd_geolsrc", -- Source de la géolocalisation pour préciser la source si nécessaire
        "nd_creadat"::text as "{{ sourceFieldPrefix }}nd_creadat__s", -- 2021/03/09 00:00:00.000 Date de création de l'objet dans le S.I.
        "nd_majdate"::text as "{{ sourceFieldPrefix }}nd_majdate__s", -- Dernière date de mise à jour de l'objet dans le S.I.
        "nd_majsrc"::text as "{{ sourceFieldPrefix }}nd_majsrc", -- Source utilisée pour la mise à jour
        "nd_abddate"::text as "{{ sourceFieldPrefix }}nd_abddate__s", -- Date d'abandon (fin de validité) de l'objet dans le S.I.
        "nd_abdsrc"::text as "{{ sourceFieldPrefix }}nd_abdsrc", -- Motif de l'abandon de l'objet

        {{ schema }}.to_text_or_null(split_part("nd_voie"::text, '/'::text, 1)) as "{{ sourceFieldPrefix }}com_code", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        {{ schema }}.to_text_or_null(split_part("nd_voie"::text, '/'::text, 2)) as "{{ sourceFieldPrefix }}com_name", --  enrichissement mminimal pour rendre code insee lisible ?
        {{ schema }}.to_text_or_null(split_part("nd_voie"::text, '/'::text, 3)) as "{{ sourceFieldPrefix }}rue",
        {{ schema }}.to_text_or_null(split_part("nd_voie"::text, '/'::text, 4)) as "{{ sourceFieldPrefix }}numero" -- ??

        -- TODO autocompléter colonnes ! NON requiert UNION avec star donc pas dans translated

    from typed_names_renamed

), specific_parsed as (

    select
        renamed.*,
        --uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns }}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}IdSupportAerien", -- NOO rather in add_generic_fields()
        -- TODO TODO Q geometry
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}DateConstruction__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ fieldPrefix }}DateConstruction",
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}pt_secu__s") as "{{ sourceFieldPrefix }}pt_secu",
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}HauteurAppui__s") as "{{ fieldPrefix }}HauteurAppui",
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}pt_a_passa__s") as "{{ sourceFieldPrefix }}pt_a_passa", -- 0 0 pour passage de câbles uniquement
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}pt_a_strat__s") as "{{ sourceFieldPrefix }}pt_a_strat",
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}Azimut__s") as "{{ fieldPrefix }}Azimut",
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}pt_detec__s") as "{{ sourceFieldPrefix }}pt_detec",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}pt_datemes__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}pt_datemes",
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}pt_a_dan__s") as "{{ sourceFieldPrefix }}pt_a_dan",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}pt_a_dtetu__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}pt_a_dtetu",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}pt_creadat__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}pt_creadat",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}pt_majdate__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}pt_majdate",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}pt_abddate__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}pt_abddate",
        -- nd :
        {{ schema }}.to_numeric_or_null("{{ sourceFieldPrefix }}nd_geolqlt__s") as "{{ sourceFieldPrefix }}nd_geolqlt",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}nd_creadat__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}nd_creadat",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}nd_majdate__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}nd_majdate",
        {{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}nd_abddate__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text) as "{{ sourceFieldPrefix }}nd_abddate"

    from renamed

), translated as (

    select
        specific_parsed.*,
        case when "{{ sourceFieldPrefix }}nd_geolqlt" is NULL then NUll when "{{ sourceFieldPrefix }}nd_geolqlt" > 1.5 then 'C' when "{{ sourceFieldPrefix }}nd_geolqlt" > 0.4 then 'B' else 'A' end as "{{ fieldPrefix }}ClassePrecision",
        --typephy."Valeur" as "{{ fieldPrefix }}TypePhysique", NON toujours APPUI dans le cas d'usage
        nature."Valeur" as "{{ fieldPrefix }}Nature",
        --NULL as "{{ fieldPrefix }}Materiau", -- pas de Materiau ; NULL pour _computed
        NULL as "{{ fieldPrefix }}CompositionAppui", -- pas de Composition
        "{{ sourceFieldPrefix }}pt_a_struc" as "{{ fieldPrefix }}StructureAppui", -- TODO Structure est contraint mais pt_a_struc non !!'
        "{{ sourceFieldPrefix }}pt_a_dan" * 10 as "{{ fieldPrefix }}EffortTransversal"
        -- TODO Q BandeauVert Date de l'étude de charge ??

    from specific_parsed
        {#
        left join {{ ref('l_pointaccueil_typephysique') }} typephy -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on specific_parsed."{{ sourceFieldPrefix }}pt_typephy" = typephy."{{ sourceFieldPrefix }}pt_typephy"
        #}
        left join {{ ref('l_pointaccueil_nature') }} nature -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on specific_parsed."{{ sourceFieldPrefix }}pt_nature" = nature."{{ sourceFieldPrefix }}pt_nature"

), computed as (
    {#{ apcom_supportaerien_translation__computed("translated") }#}
    select
        translated.*,
        'APPUI' as "{{ fieldPrefix }}TypePhysique", -- toujours dans le cas d'usage (OSM : toujours pole ou tower)
        nature."{{ fieldPrefix }}Materiau" as "{{ fieldPrefix }}Materiau" -- 'bois'

    from translated
        left join {{ ref('l_pointaccueil_nature') }} nature -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
            on translated."{{ fieldPrefix }}Nature" = nature."Valeur"

), with_generic_fields as (
    {{ fdr_francedatareseau.add_generic_fields('computed', fieldPrefix, fdr_namespace, src_priority) }}
)

select * from with_generic_fields

{% endmacro %}