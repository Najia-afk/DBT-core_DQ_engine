{#- Macros communes des projets dbt générés (voir engine/lanes/elt/README.md). -#}

{#- Lecture d'une requête PostgreSQL (schéma d'état) : `select * from {% call pg_q() %} ... {% endcall %}` -#}
{% macro pg_q() -%}
postgres_query('pg', $mdm$ {{ caller() }} $mdm$)
{%- endmacro %}

{#- Composantes connexes (FastSV : accrochage des racines au plus petit voisin + raccourcis de
    pointeur). nodes(nid BIGINT), edges(a BIGINT, b BIGINT) avec les deux sens ; résultat : table
    `out` (nid, lab) où lab = plus petit nid de la composante. Chaque tour est une suite de jointures
    à hachage en O(N + E) et le nombre de tours est ~log2(diamètre) : pas d'énumération de chemins,
    donc pas d'explosion sur les très gros clusters. -#}
{% macro components(nodes, edges, out) -%}
  {%- if execute -%}
    {%- do run_query("create or replace table " ~ out ~ " as select nid, nid as p from " ~ nodes) -%}
    {%- set ns = namespace(done=false) -%}
    {%- for i in range(200) -%}
      {%- if not ns.done -%}
        {#- grand-parent de chaque nœud, plus petit grand-parent parmi ses voisins -#}
        {%- do run_query(
          "create or replace table " ~ out ~ "_gf as "
          ~ "select a.nid, b.p as gp from " ~ out ~ " a join " ~ out ~ " b on b.nid = a.p") -%}
        {%- do run_query(
          "create or replace table " ~ out ~ "_m as "
          ~ "select e.a as nid, min(g.gp) as mp from " ~ edges ~ " e "
          ~ "join " ~ out ~ "_gf g on g.nid = e.b group by e.a") -%}
        {%- do run_query(
          "create or replace table " ~ out ~ "_h as "
          ~ "select l.p as nid, min(m.mp) as hp from " ~ out ~ " l "
          ~ "join " ~ out ~ "_m m on m.nid = l.nid group by l.p") -%}
        {%- do run_query(
          "create or replace table " ~ out ~ "_next as "
          ~ "select l.nid, least(l.p, g.gp, coalesce(m.mp, l.p), coalesce(h.hp, l.p)) as p "
          ~ "from " ~ out ~ " l join " ~ out ~ "_gf g on g.nid = l.nid "
          ~ "left join " ~ out ~ "_m m on m.nid = l.nid "
          ~ "left join " ~ out ~ "_h h on h.nid = l.nid") -%}
        {%- set changed = run_query(
          "select count(*) from " ~ out ~ "_next n join " ~ out ~ " l on l.nid = n.nid where n.p <> l.p"
        ).columns[0].values()[0] -%}
        {%- do run_query("create or replace table " ~ out ~ " as select * from " ~ out ~ "_next") -%}
        {%- if changed == 0 -%}{%- set ns.done = true -%}{%- endif -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if not ns.done -%}
      {{ exceptions.raise_compiler_error("components did not converge") }}
    {%- endif -%}
    {%- for suffix in ["_gf", "_m", "_h", "_next"] -%}
      {%- do run_query("drop table if exists " ~ out ~ suffix) -%}
    {%- endfor -%}
  {%- endif -%}
  select nid, p as lab from {{ out }}
{%- endmacro %}
