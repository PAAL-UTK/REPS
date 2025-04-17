import typer
from ..pipeline.ingest import ingest_subject
from ..config import settings
from ..pipeline.validate import run as validate_run
from rich.console import Console

app = typer.Typer(help="REPS CLI")
console = Console()


@app.command()
def ingest(
    subject_id: str = typer.Option(
        None, "--subject-id", "-s", help="Single participant ID"
    ),
    all_: bool = typer.Option(False, "--all", help="Ingest every raw subject found"),
):
    if all_ and subject_id:
        typer.echo("Choose --subject-id OR --all, not both.")
        raise typer.Exit(1)
    if not all_ and not subject_id:
        typer.echo("Provide --subject-id or use --all.")
        raise typer.Exit(1)

    if all_:
        acc_dir = settings.RAW_ROOT / "acc"
        ids = [
            p.name.split("_")[0].split("-")[1]
            for p in acc_dir.glob("REPS-*_acc.parquet")
        ]
        for sid in ids:
            ingest_subject(sid)
    else:
        ingest_subject(subject_id)


# ──────────────────────────────────────────────────────────────
# NEW: data‑warehouse validation
# $ reps validate
# exits 0 on success, 1 on failure
# ──────────────────────────────────────────────────────────────


@app.command("validate")
def validate_data() -> None:
    """Run warehouse‑sanity checks."""
    errors = validate_run()
    if errors:
        console.print("[red bold]DATA VALIDATION FAILED[/red bold]")
        for err in errors:
            console.print(err)
        raise typer.Exit(code=1)
    console.print("[green]Warehouse looks clean.[/green]")


def main():
    app()


if __name__ == "__main__":
    main()
