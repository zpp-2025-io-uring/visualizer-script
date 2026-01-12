import pytest

from main import main


@pytest.fixture()
def invoke_main(capsys):
    """Fixture that returns a callable to invoke `main` and capture output.

    Usage in tests:
        def test_something(invoke_main):
            out, err = invoke_main(["--help"])
    """

    def _invoke(argv: list[str]) -> tuple[str, str]:
        main(argv)
        (out, err) = capsys.readouterr()
        print("Captured stdout:", out)
        print("Captured stderr:", err)
        return (out, err)

    return _invoke
