
from detective.manifest_helper import simple, contextual, Predicate, SimplePredicate, ContextualPredicate
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ManifestNode
from functools import cache
from collections import deque

TABLE = "table"
VIEW = "view"
INCREMENTAL = "incremental"
EPHEMERAL = "ephemeral"
DYNAMIC_TABLE = "dynamic_table"


# These materializations result in a table, new data upstream does not appear until the next `dbt run`
TABLE_MATERIALIZATIONS = ("table", "incremental",)



def is_materialized_as(*materialization: str) -> SimplePredicate:
    @simple
    def p(node: ManifestNode) -> bool:
        return node.get_materialization() in materialization
    return p


@simple
def has_downstream_target_lag(node: ManifestNode) -> bool:
    return node.config.get("target_lag") == "DOWNSTREAM"


@contextual
def is_last_of_same_materialization(node: ManifestNode, manifest: Manifest) -> bool:
    materialization = node.get_materialization()
    for child_name in manifest.child_map[node.unique_id]:
        child_node = manifest.nodes.get(child_name)
        if child_node is not None and child_node.get_materialization() == materialization:
            return False
    return True


@contextual
def terminal_dynamic_table_with_DOWNSTREAM_lag(node: ManifestNode, manifest: Manifest) -> bool:
    return is_materialized_as(DYNAMIC_TABLE)(node) \
        and has_downstream_target_lag(node) \
        and is_last_of_same_materialization(node, manifest)


def has_upstream_materialized_as(*materializations: str) -> ContextualPredicate:
    confirmed_nodes = set()
    visited_nodes = set()
    @contextual
    def depth_first(node: ManifestNode, manifest: Manifest) -> bool:
        que = deque(manifest.parent_map[node.unique_id])
        while que:
            node_id = que.pop()
            if node_id in confirmed_nodes:
                return True
            if node_id not in visited_nodes:
                try:
                    current_node = manifest.nodes[node_id]
                except KeyError:
                    continue
                visited_nodes.add(current_node.unique_id)
                if current_node.get_materialization() in materializations:
                    confirmed_nodes.add(node_id)
                    return True
                que.extend(n_id for n_id in manifest.parent_map[current_node.unique_id]
                           if n_id not in visited_nodes)

        return False

    return depth_first


@contextual
def static_table_with_upstream_dynamic_table(node: ManifestNode, manifest: Manifest) -> bool:
    return is_materialized_as(TABLE, INCREMENTAL)(node) \
        and has_upstream_materialized_as(DYNAMIC_TABLE)(node, manifest)


NAMED_PREDICATES: dict[str, Predicate] = {
    "is-dynamic-table": is_materialized_as(DYNAMIC_TABLE),
    "has-downstream-target-lag": has_downstream_target_lag,
    "is-last-of-same-materialization": is_last_of_same_materialization,
}

NAMED_SEARCHES: dict[str, Predicate] = {
    "terminal-dynamic-table-with-DOWNSTREAM-lag": terminal_dynamic_table_with_DOWNSTREAM_lag,
    "static-table-with-upstream-dynamic-table": static_table_with_upstream_dynamic_table,
}
