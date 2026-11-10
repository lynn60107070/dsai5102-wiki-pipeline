"""Draw the dbt lineage graph for the report: lake files -> staging -> core -> marts -> Postgres serving.

Reads dbt's manifest.json and prints a Graphviz graph, one column per layer. Run:
  docker compose exec airflow-scheduler bash -c 'cd $DBT_PROJECT_DIR && /opt/airflow/dbt-venv/bin/dbt parse --target-path /tmp/lineage'
  docker cp wiki-pipeline-airflow-scheduler-1:/tmp/lineage/manifest.json .
  python scripts/lineage_figure.py manifest.json > lineage.dot
  dot -Tpdf lineage.dot -o paper/figures/lineage.pdf   (and -Tpng -Gdpi=200 for slides)
A separate --target-path keeps the parse away from the hourly run's files.
"""
import json
import sys

# The staging views read the lake directly (read_parquet), so these inputs are not dbt sources.
LAKE_INPUTS = {
    "stg_edits": "lake: clean/edits\n(Parquet, from the stream)",
    "stg_pageviews": "lake: raw/pageviews\narticles.parquet",
    "stg_wiki_views": "lake: raw/pageviews\nwiki_totals.parquet",
}
COLUMNS = ["lake", "staging", "seed", "core", "marts", "serving"]
FILL = {"lake": "#e8e8e8", "staging": "#dbe8f5", "seed": "#f2f2f2", "core": "#fbe5c8",
        "marts": "#d9efd9", "serving": "#e8e8e8"}
STYLE = {"view": "dashed,filled", "incremental": "bold,filled"}  # tables: plain filled


def layer(node: dict) -> str:
    return "seed" if node["resource_type"] == "seed" else node["fqn"][1]


def main():
    manifest = json.load(open(sys.argv[1]))
    nodes = {key: n for key, n in manifest["nodes"].items() if n["resource_type"] in ("model", "seed")}
    columns = {name: [] for name in COLUMNS}
    edges = []
    for key, node in nodes.items():
        columns[layer(node)].append(key)
        edges += [(parent, key) for parent in node.get("depends_on", {}).get("nodes", []) if parent in nodes]
        if node["name"] in LAKE_INPUTS:
            lake_key = f"lake.{node['name']}"
            columns["lake"].append(lake_key)
            edges.append((lake_key, key))
        if layer(node) == "marts":
            edges.append((key, "serving"))
    columns["serving"].append("serving")

    print("digraph lineage {")
    print('  rankdir=LR; nodesep=0.12; ranksep=0.45; splines=true;')
    print('  node [shape=box, style=filled, fontname="Helvetica", fontsize=10, height=0.3, margin="0.08,0.03"];')
    print('  edge [color="#777777", arrowsize=0.5, penwidth=0.7];')
    for column in COLUMNS:
        print(f'  {{ rank=same; node [fillcolor="{FILL[column]}"];')
        for key in sorted(columns[column]):
            if column == "lake":
                label, style = LAKE_INPUTS[key.split(".", 1)[1]], "filled,rounded"
            elif column == "serving":
                label, style = "Postgres serving\n(export_marts, read by Metabase)", "filled,rounded"
            else:
                node = nodes[key]
                label, style = node["name"], STYLE.get(node["config"].get("materialized"), "filled")
            print(f'    "{key}" [label="{label}", style="{style}"];')
        print("  }")
    for parent, child in edges:
        print(f'  "{parent}" -> "{child}";')
    print("}")


if __name__ == "__main__":
    main()
