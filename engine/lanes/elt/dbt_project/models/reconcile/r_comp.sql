{{ config(materialized='table') }}
{#- Composantes connexes par propagation du plus petit label avec saut de pointeur (macro
    `components`) : pas de CTE récursive énumérant des chemins, donc pas d'explosion sur les gros
    clusters. -#}
{{ components(ref('r_nodes'), ref('r_edges'), 'r_comp_lab') }}
