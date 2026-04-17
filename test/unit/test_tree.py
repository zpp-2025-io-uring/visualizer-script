from yaml import safe_dump, safe_load

from tree import TreeDict

VALUES = {(("a", "b"), 1), (("a", "c"), 2), (("d",), 3)}


def test_yaml_serialize_and_deserialize() -> None:
    tm: TreeDict[int] = TreeDict()
    for path, value in VALUES:
        tm[path] = value

    # Act
    yaml_str = safe_dump(tm)
    loaded_tm = safe_load(yaml_str)

    # Assert
    assert isinstance(loaded_tm, TreeDict)
    for path, value in VALUES:
        assert loaded_tm[path] == value

    for path, value in loaded_tm.items():
        assert (path, value) in VALUES


def test_items() -> None:
    tm: TreeDict[int] = TreeDict()
    for path, value in VALUES:
        tm[path] = value

    items = set(tm.items())
    expected_items = {(("a", "b"), 1), (("a", "c"), 2), (("d",), 3)}

    assert items == expected_items


def test_setdefault_for_existing_value() -> None:
    tm: TreeDict[int] = TreeDict()
    old_value = 10
    tm[("x", "y")] = old_value

    result = tm.setdefault(("x", "y"), 20)

    assert result == old_value
    assert tm[("x", "y")] == old_value


def test_setdefault_for_new_value() -> None:
    tm: TreeDict[int] = TreeDict()
    default_value = 30

    result = tm.setdefault(("m", "n"), default_value)

    assert result == default_value
    assert tm[("m", "n")] == default_value


def test_contains() -> None:
    tm: TreeDict[int] = TreeDict()
    tm[("p", "q")] = 42

    assert ("p", "q") in tm
    assert ("p",) not in tm
    assert ("q",) not in tm
    assert ("p", "q", 42) not in tm
    assert ("x", "y") not in tm


def test_get_with_default_comparator() -> None:
    tm: TreeDict[int] = TreeDict()
    value = 99
    existing_path = ("foo", "bar")
    nonexistent_path = ("foo", "baz")
    tm[existing_path] = value

    assert tm.get(existing_path) == value
    assert tm.get(nonexistent_path) is None


def test_get_with_custom_comparator() -> None:
    tm: TreeDict[int] = TreeDict()

    def case_insensitive_comparator(x: str, y: str) -> bool:
        return x.lower() == y.lower()

    value = 99
    existing_path = ("Foo", "Bar")
    tm[existing_path] = value

    paths_to_test = [
        ("foo", "bar"),
        ("FOO", "BAR"),
        ("FoO", "bAr"),
    ]

    # Healthcheck: default comparator should not find the value with different case
    for path in paths_to_test:
        assert tm.get(path) is None

    # Custom comparator should find the value regardless of case
    for path in paths_to_test:
        assert tm.get(path, comparator=case_insensitive_comparator) == value


def test_get_with_incomplete_path() -> None:
    tm: TreeDict[int] = TreeDict()
    value = 99
    existing_path = ("foo", "bar", "baz")
    tm[existing_path] = value

    assert tm.get(existing_path) == value
    for i in range(0, len(existing_path)):
        assert tm.get(existing_path[:i]) is None


def test_keys() -> None:
    tm: TreeDict[int] = TreeDict()
    for path, value in VALUES:
        tm[path] = value

    keys = tm.keys()
    assert set(keys) == {path for path, _ in VALUES}


def test_len() -> None:
    tm: TreeDict[int] = TreeDict()
    current_len = 0

    def increment_len(path: tuple, value: int) -> None:
        nonlocal current_len, tm
        tm[path] = value
        current_len += 1

    for path, value in VALUES:
        increment_len(path, value)
        assert len(tm) == current_len


def test_yaml_deserialize_with_duplicate_keys() -> None:
    yaml_str = """
    a:
      b: 1
      b: 2
    """
    try:
        safe_load(yaml_str)
        assert False, "Expected ValueError for duplicate keys, but no exception was raised."
    except ValueError as e:
        assert "Duplicate key 'b'" in str(e), f"Unexpected error message: {str(e)}"
