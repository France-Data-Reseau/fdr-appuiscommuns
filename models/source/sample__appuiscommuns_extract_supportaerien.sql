{#
Normalisation vers le modèle de données du cas d'usage "eau potable" des données de type canalisation de la source d'exemple embarquée "echantillon 3"

- OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la __definition) ?
#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien_osmgeodatamine_powersupports_extract' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set ns = 'supportaerien.appuiscommuns.francedatareseau.fr' %} -- ?
{% set typeName = 'SupportAerien' %}
{% set sourcePrefix = 'osmpowersupports' %} -- ?
{% set prefix = 'appuiscommunssupp' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '__' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('osmgeodatamine_powersupports_extract') }} -- TODO raw_

),

renamed as (

    select
        'osmgeodatamine_powersupports_extract' as "{{ fieldPrefix }}src_name", -- source name (else won't have it anymore once unified with other sources)
        --id as "{{ fieldPrefix }}src_index", -- index in source
        "osm_id" as "{{ fieldPrefix }}src_id", -- source own id
        "X", -- as "{{ sourceFieldPrefix }}x",
        "Y", -- as "{{ sourceFieldPrefix }}x",
        "utility" as "{{ sourceFieldPrefix }}utility", -- power
        "nature" as "{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        "operator" as "{{ fieldPrefix }}Gestionnaire",
        "material" as "{{ sourceFieldPrefix }}material", -- TODO dict conv
        "height" as "{{ fieldPrefix }}HauteurTotal", -- plutôt que HauteurAppui (?) TODO Hauteur ! hauteur ? __m ??
        "reference" as "{{ fieldPrefix }}CodeExterne", -- ?? 101, 87, 37081ER073...
        "line_attachment" as "{{ sourceFieldPrefix }}line_attachment", -- ? suspension, pin, anchor...
        "line_management" as "{{ sourceFieldPrefix }}line_management", -- ? split, branch, cross...
        "transition" as "{{ sourceFieldPrefix }}transition", -- ? yes
        "com_insee" as "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        "com_insee" as "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        "com_insee" as "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        "com_nom" as "{{ fieldPrefix }}commune_nom", -- enrichissement mminimal pour rendre code insee lisible ?
        "com_nom" as "{{ fieldPrefix }}fdrcommune__nom" -- TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)

    from source

),

parsed as (

    select
        "{{ fieldPrefix }}src_name",
        --"{{ fieldPrefix }}src_index",
        "{{ fieldPrefix }}src_id",
        uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns }}}}'), "{{ fieldPrefix }}src_id") as "{{ fieldPrefix }}Id",
        ST_GeomFROMText('POINT(' || cast("X" as text) || ' ' || cast("Y" as text) || ')', 4326) as geometry, -- OU prefix ? forme ?? ou /et "Geom" ?
        "{{ sourceFieldPrefix }}utility", -- power
        "{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        "{{ fieldPrefix }}Gestionnaire",
        "{{ sourceFieldPrefix }}material", -- TODO dict conv
        "{{ fieldPrefix }}HauteurTotal", -- TODO Hauteur ! hauteur ? __m ??
        "{{ fieldPrefix }}CodeExterne", -- 101, 87, 37081ER073...
        "{{ sourceFieldPrefix }}line_attachment", -- suspension, pin, anchor, pulley, (pin)|(anchor), anchor|pin, suspension | anchor, anchor;pin, (suspension)|(suspension), yes...
        "{{ sourceFieldPrefix }}line_management", -- split, branch, cross...
        "{{ sourceFieldPrefix }}transition", -- yes
        "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        "{{ fieldPrefix }}commune_nom", -- enrichissement mminimal pour rendre code insee lisible ?
        "{{ fieldPrefix }}fdrcommune__nom" -- TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)

    from renamed

),

reconciled as (

    select
        "{{ fieldPrefix }}src_name",
        --"{{ fieldPrefix }}src_index",
        "{{ fieldPrefix }}src_id",
        "{{ fieldPrefix }}Id",
        geometry, -- OU prefix ? forme ??  ou /et "Geom" ?
        "{{ sourceFieldPrefix }}utility", -- power
        "{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        "{{ fieldPrefix }}Gestionnaire",
        {{ ref('l_appuisaeriens_materiau__osmgeodatamine') }}."Valeur" as "{{ fieldPrefix }}Materiau", -- TODO dict conv
        "{{ fieldPrefix }}HauteurTotal", -- TODO Hauteur ! hauteur ? __m ??
        "{{ fieldPrefix }}CodeExterne", -- 101, 87, 37081ER073...
        "{{ sourceFieldPrefix }}line_attachment", -- suspension, pin, anchor...
        "{{ sourceFieldPrefix }}line_management", -- split, branch, cross...
        "{{ sourceFieldPrefix }}transition", -- yes
        "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        "{{ fieldPrefix }}commune_nom", -- enrichissement mminimal pour rendre code insee lisible ?
        "{{ fieldPrefix }}fdrcommune__nom" -- TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)

    from parsed
        join {{ ref('l_appuisaeriens_materiau__osmgeodatamine') }}
            on parsed.{{ sourceFieldPrefix }}material = {{ ref('l_appuisaeriens_materiau__osmgeodatamine') }}.{{ sourceFieldPrefix }}material
            
    
),

computed as (

    select
        "{{ fieldPrefix }}src_name",
        --"{{ fieldPrefix }}src_index",
        "{{ fieldPrefix }}src_id",
        "{{ fieldPrefix }}Id",
        geometry, -- OU prefix ? forme ??  ou /et "Geom" ?
        "{{ sourceFieldPrefix }}utility", -- power
        "{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        'APPUI' as "{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        {{ ref('l_pointaccueil_nature') }}."{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        "{{ fieldPrefix }}Gestionnaire",
        "{{ fieldPrefix }}Materiau", -- TODO dict conv
        "{{ fieldPrefix }}HauteurTotal", -- TODO Hauteur ! hauteur ? __m ??
        "{{ fieldPrefix }}CodeExterne", -- 101, 87, 37081ER073...
        "{{ sourceFieldPrefix }}line_attachment", -- suspension, pin, anchor...
        "{{ sourceFieldPrefix }}line_management", -- split, branch, cross...
        "{{ sourceFieldPrefix }}transition", -- yes
        "{{ fieldPrefix }}commune_insee_id", -- sert à enriched qui est indépendant de la source, donc sourceFieldPrefix ne suffirait pas ; alternative plus précise
        "fdrcommune__insee_id", -- alternative plus facile à réconcilier
        "{{ fieldPrefix }}fdrcommune__insee_id", -- TODO OU les deux OUI (comme un chemin)
        "{{ fieldPrefix }}commune_nom", -- enrichissement mminimal pour rendre code insee lisible ?
        "{{ fieldPrefix }}fdrcommune__nom" -- TODO OU OUI (et le fait que insee_id est déjà un id / unique permettra de savoir qu'il n'y a pas besoin de nom pour réconcillier)

    from reconciled
        join {{ ref('l_pointaccueil_nature') }}
            on reconciled."{{ fieldPrefix }}Materiau" = {{ ref('l_pointaccueil_nature') }}."Valeur"
            
    
)

select * from computed
