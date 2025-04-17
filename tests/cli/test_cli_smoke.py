from reps.cli import main as cli_main


def test_validate_help(runner):
    result = runner.invoke(cli_main.app, ["validate", "--help"])
    assert result.exit_code == 0
    assert "Run warehouse‑sanity checks" in result.output
