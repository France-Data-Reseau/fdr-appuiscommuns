{#
Unification des données normalisées de toutes les sources de type appuiscommuns.supportaerien
#}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'appuiscommuns_supportaerien' %} -- _2021 ? from this file ? prefix:typeName ?
{% set typeName = 'SupportAerien' %}
{% set prefix = 'appuiscommunssupp' %} -- ?
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '__' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

{#
Normally we would not use samples

Union using dbt_utils helper :
- include=dbt_utils.star(...) allows get exactly the fields of an official type definition (ex. provided bby a CSV seed sample)
- column_override={"geometry": "geometry"} is required else syntax error : cast("geometry" as USER-DEFINED) as "geo...
see https://github.com/dbt-labs/dbt-utils#union_relations-source
#}
{{ dbt_utils.union_relations(
    relations=[ref('osmgeodatamine_powsupp__appuiscommuns_supportaerien')],
    include=dbt_utils.star(ref('appuiscommuns_supportaerien__definition')),
    column_override={"geometry": "geometry"}
) }}

{#
Alternative : explicit SELECT * or all fields explicitly UNION...
with source as (

    select
        "{{ fieldPrefix }}src_name",
        --"{{ fieldPrefix }}src_index",
        "{{ fieldPrefix }}src_id",
        "{{ fieldPrefix }}Id",
        geometry, -- OU prefix ? forme ??
        --"{{ sourceFieldPrefix }}utility", -- power
        --"{{ sourceFieldPrefix }}nature", -- pole, tower TODO dict conv
        "{{ fieldPrefix }}TypePhysique", -- vu que toujours pole ou tower (ou CASE WHEN ?)
        "{{ fieldPrefix }}Nature", -- 'POTEAU BOIS'
        "{{ fieldPrefix }}Gestionnaire",
        "{{ fieldPrefix }}Materiau", -- TODO dict conv
        "{{ fieldPrefix }}HauteurAppui", -- TODO Hauteur ! hauteur ? __m ??
        "{{ fieldPrefix }}CodeExterne", -- 101, 87, 37081ER073...
        --"{{ sourceFieldPrefix }}line_attachment", -- suspension, pin, anchor... MAIS QUE CompositionAppui (plein), StructureAppui (moise)
        --"{{ sourceFieldPrefix }}line_management", -- split, branch, cross... MAIS QUE CompositionAppui (plein), StructureAppui (moise)
        --"{{ sourceFieldPrefix }}transition", -- yes
        "{{ fieldPrefix }}fdrcommune__insee_id",
        "{{ fieldPrefix }}fdrcommune__nom"
        
        from {{ ref('sample__appuiscommuns_extract_supportaerien') }}
    
    --UNION...

)

select * from source
#}