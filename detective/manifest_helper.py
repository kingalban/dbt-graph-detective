from typing import Callable, Generator, TypeGuard, get_type_hints, get_args
from functools import partial, cached_property

from dbt.contracts.graph.manifest import Manifest
from dbt.artifacts.schemas.manifest import WritableManifest
from dbt.contracts.graph.nodes import ManifestNode

SimplePredicate = Callable[[ManifestNode], bool]
ContextualPredicate = Callable[[ManifestNode, Manifest], bool]
Predicate = SimplePredicate | ContextualPredicate


FUNC_TYPE_MARKER = "__predicate_type__"

def simple(func: SimplePredicate) -> SimplePredicate:
    if tuple(get_type_hints(func).values()) == get_args(SimplePredicate)[0]:
        raise TypeError(f"function {func!r} does not have compatible types with 'SimplePredicate'")
    setattr(func, FUNC_TYPE_MARKER, SimplePredicate)
    return func

def contextual(func: ContextualPredicate) -> ContextualPredicate:
    if tuple(get_type_hints(func).values()) == get_args(ContextualPredicate)[0]:
        raise TypeError(f"function {func!r} does not have compatible types with 'SimplePredicate'")
    setattr(func, FUNC_TYPE_MARKER, ContextualPredicate)
    return func

def is_simple(func: Predicate) -> TypeGuard[SimplePredicate]:
    return getattr(func, FUNC_TYPE_MARKER) == SimplePredicate

def is_contextual(func: Predicate) -> TypeGuard[ContextualPredicate]:
    return getattr(func, FUNC_TYPE_MARKER) == ContextualPredicate


class ManifestSearcher:
    def __init__(self, manifest_path: str) -> None:
        self.manifest_path = manifest_path

    @cached_property
    def manifest(self):
        writable_manifest = WritableManifest.read_and_check_versions(self.manifest_path)
        manifest = Manifest.from_writable_manifest(writable_manifest)
        manifest.build_parent_and_child_maps()
        return manifest

    def _prepare_call(self, func: Predicate) -> SimplePredicate:
        """Inspect predicate's annotations and inject the manifest if needed."""
        if is_simple(func):
            return func
        elif is_contextual(func):
            for keyword, _type in get_type_hints(func).items():
                if _type is Manifest and keyword != "return":
                    return partial(func, **{keyword: self.manifest})
        raise RuntimeError(f"function {func!r} was not marked as a @contextual or @simple predicate")

    def search(self, *predicates_funcs: Predicate) -> Generator[ManifestNode, None, None]:
        """Walks the manifest and yields nodes that match all predicates."""
        prepared_predicates = tuple(self._prepare_call(pred) for pred in predicates_funcs)
        for unique_id, node in self.manifest.nodes.items():
            if all(pred(node) for pred in prepared_predicates):
                yield node
