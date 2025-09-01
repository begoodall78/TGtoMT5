import os, sqlite3, json
from collections import Counter
import typer

from app.common.logging_config import setup_logging
from app.storage import init_db, enqueue
from app.infra.actions_runner import run_forever, process_once
from app.models import Action, Leg
from app.processing import build_actions_from_message

app = typer.Typer(no_args_is_help=True)

# ---------------- Commands ----------------

@app.command()
def run(log_level: str = typer.Option("INFO", "--log-level", "-l",
                                      help="DEBUG, INFO, WARNING, ERROR")):
    """Run the actions runner (process SQLite queue)."""
    setup_logging(log_level)
    init_db()
    run_forever()

@app.command()
def drain(batch: int = 64,
          log_level: str = typer.Option("INFO", "--log-level", "-l")):
    """Process up to <batch> pending actions and exit."""
    setup_logging(log_level)
    init_db()
    n = process_once(batch=batch)
    typer.echo(f"Processed {n} actions")

@app.command()
def smoke(symbol: str = "XAUUSD", side: str = "SELL", volume: float = 0.01,
          log_level: str = typer.Option("INFO", "--log-level", "-l")):
    """Enqueue an OPEN+CLOSE smoke test."""
    setup_logging(log_level)
    init_db()
    leg = Leg(leg_id="SMOKE#1", symbol=symbol, side=side.upper(), volume=volume, tag="SMOKE")
    open_action = Action(action_id="smoke-open", type="OPEN", legs=[leg], source_msg_id="SMOKE")
    close_action = Action(action_id="smoke-close", type="CLOSE", legs=[leg], source_msg_id="SMOKE")
    enqueue(open_action); enqueue(close_action)
    typer.echo("Smoke actions enqueued.")

@app.command()
def status(n: int = 10,
           show_files: bool = False,
           actions_dir: str = typer.Option(None, help="Override MT5_ACTIONS_DIR"),
           log_level: str = typer.Option("INFO", "--log-level", "-l")):
    """Show queue/execution counts, leg totals, and last N actions."""
    setup_logging(log_level)
    init_db()
    db = os.environ.get("APP_DB_PATH", "data/app.db")
    con = sqlite3.connect(db)

    counts = dict(con.execute("SELECT status, COUNT(*) FROM queue GROUP BY status").fetchall())
    ex_count = con.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
    rows = con.execute("SELECT payload FROM queue ORDER BY ts DESC").fetchall()

    types = Counter()
    total_legs = 0
    for (p,) in rows:
        d = json.loads(p.decode())
        types[d["type"]] += 1
        total_legs += len(d["legs"])

    print("queue:", counts)
    print("executions:", ex_count)
    print("action_types:", dict(types))
    print("total_legs_across_actions:", total_legs)

    print("\nlast actions:")
    for (p,) in rows[:n]:
        d = json.loads(p.decode())
        print(f"{d['type']}\tlegs={len(d['legs'])}\tid={d['action_id']}")

    if show_files:
        adir = actions_dir or os.environ.get("MT5_ACTIONS_DIR") or os.path.join(os.getcwd(), "mt5_actions")
        try:
            files = [f for f in os.listdir(adir) if f.lower().endswith(".csv")]
            print(f"\nmt5_actions_dir: {adir}")
            print(f"csv_files_found: {len(files)} (expected ~{total_legs})")
        except Exception as e:
            print(f"\nmt5_actions_dir error: {e}")

@app.command()
def preview(text: str,
            legs: int = 5,
            volume: float = 0.01,
            edit: bool = False,
            log_level: str = typer.Option("INFO", "--log-level", "-l")):
    """Preview how a message would parse & map to legs (no DB, no files)."""
    setup_logging(log_level)
    # no DB needed, but init is cheap/safe
    init_db()
    acts = build_actions_from_message(
        source_msg_id="preview",
        text=text,
        is_edit=bool(edit),
        legs_count=legs,
        leg_volume=volume
    )
    if not acts:
        typer.echo("No action parsed.")
        raise typer.Exit(0)
    a = acts[0]
    rows = []
    for i, L in enumerate(a.legs, 1):
        rows.append({
            "leg": i,
            "leg_id": L.leg_id,
            "side": L.side,
            "entry": L.entry,
            "tp": L.tp,
            "sl": L.sl,
            "tag": L.tag
        })
    typer.echo(json.dumps({
        "type": a.type,
        "legs": rows
    }, indent=2))

if __name__ == "__main__":
    app()
