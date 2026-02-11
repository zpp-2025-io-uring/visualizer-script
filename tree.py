from collections.abc import Callable, Iterator
from typing import Generic, TypeVar

import yaml
from yamlable import YamlAble, yaml_info

T = TypeVar("T")


@yaml_info("leaf")
class _Leaf(Generic[T], YamlAble):
    def __init__(self, value: T) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Leaf({self.value})"


@yaml_info("tree_dict")
class TreeDict(Generic[T], YamlAble):
    def __init__(self) -> None:
        self.metrics: dict = {}

    def get_metrics(self) -> dict:
        return self.metrics

    def items(self) -> Iterator[tuple[tuple[str, ...], T]]:
        """Iterate over all leaf (path, value) pairs in the metrics tree.

        Example:
            For metrics = {'io': {'read': {'256kb': 123}}}
            items() yields (('io','read','256kb'), 123)
        """

        def walk(prefix: list, data) -> Iterator[tuple[tuple[str, ...], T]]:
            if not isinstance(data, dict):
                yield (tuple(prefix), data.value)
                return
            for key, val in data.items():
                yield from walk(prefix + [key], val)

        yield from walk([], self.metrics)

    def __getitem__(self, path: tuple) -> T | dict:
        """Get the value or subtree at the given path in the metrics tree.

        If the path leads to a leaf, returns the leaf value.
        If the path leads to an internal node, returns the subtree dict.
        If the path does not exist, creates intermediate nodes as needed.
        """
        cur = self.metrics
        for part in path:
            if part not in cur:
                cur[part] = {}
            cur = cur[part]
        if isinstance(cur, dict):
            return cur
        else:
            return cur.value

    def __setitem__(self, path: tuple, value: T) -> None:
        """Set the value at the given path in the metrics tree.

        Creates intermediate nodes as needed.
        """
        cur = self.metrics
        for part in path[:-1]:
            if part not in cur:
                cur[part] = {}
            cur = cur[part]
        cur[path[-1]] = _Leaf(value)

    def setdefault(self, path: tuple, default: T) -> T:
        """Set the value at the given path if not already set, and return it.

        Creates intermediate nodes as needed.
        """
        cur = self.metrics
        for part in path[:-1]:
            if part not in cur:
                cur[part] = {}
            cur = cur[part]
        if path[-1] not in cur:
            cur[path[-1]] = _Leaf(default)
            return default
        else:
            leaf = cur[path[-1]]
            if isinstance(leaf, _Leaf):
                return leaf.value
            else:
                raise TypeError(f"Expected leaf at path {path}, found subtree.")

    def get(self, path: tuple, comparator: Callable[[str, str], bool] = lambda x, y: x == y) -> T | None:
        """Get the value at the given path if it exists and satisfies the comparator, else None."""
        cur = self.metrics
        for part in path:
            if not isinstance(cur, dict):
                return None

            matching_keys = [k for k in cur.keys() if comparator(k, part)]
            if not matching_keys:
                return None
            if len(matching_keys) > 1:
                raise ValueError(f"Multiple matching keys for {part} at path {path}: {matching_keys}")

            cur = cur[matching_keys[0]]

        if isinstance(cur, _Leaf):
            return cur.value
        else:
            # We reached a subtree instead of a leaf, so the path is incomplete
            return None

    def keys(self) -> list[tuple[str, ...]]:
        """Return a list of all paths to leaf nodes in the metrics tree."""
        return [path for path, _ in self.items()]

    def __contains__(self, path: tuple) -> bool:
        """Check if the given path exists in the metrics tree."""
        cur = self.metrics
        for part in path:
            if not isinstance(cur, dict) or part not in cur:
                return False
            cur = cur[part]
        return isinstance(cur, _Leaf)

    def __len__(self) -> int:
        """Return the number of leaf nodes in the metrics tree."""
        return sum(1 for _ in self.items())

    def __repr__(self) -> str:
        return f"TreeDict({self.metrics})"

    def __to_yaml_dict__(self) -> dict:
        return self.metrics

    @classmethod
    def __from_yaml_dict__(cls, dct, yaml_tag) -> "TreeDict":
        obj = cls()
        obj.metrics = dct
        return obj


class NoDuplicateLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        seen = set()
        mapping = []
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise ValueError(f"Duplicate key '{key}' at {key_node.start_mark}")
            seen.add(key)
            mapping.append((key, self.construct_object(value_node, deep=deep)))
        return dict(mapping)


yaml.SafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    NoDuplicateLoader.construct_mapping,
)
