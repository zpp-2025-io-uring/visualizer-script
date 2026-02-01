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
