
from detective.manifest_helper import SimplePredicate, simple, contextual, Predicate
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ManifestNode


def is_materialized_as(materialization: str) -> SimplePredicate:
    @simple
    def p(node: ManifestNode) -> bool:
        return node.get_materialization() == materialization
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
    return is_materialized_as("dynamic_table")(node) \
        and has_downstream_target_lag(node) \
        and is_last_of_same_materialization(node, manifest)


NAMED_PREDICATES: dict[str, Predicate] = {
    "is-dynamic-table": is_materialized_as("dynamic_table"),
    "has-downstream-target-lag": has_downstream_target_lag,
    "is-last-of-same-materialization": is_last_of_same_materialization,
}

NAMED_SEARCHES: dict[str, Predicate] = {
    "terminal-dynamic-table-with-DOWNSTREAM-lag": terminal_dynamic_table_with_DOWNSTREAM_lag
}
