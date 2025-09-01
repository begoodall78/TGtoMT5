from __future__ import annotations
import json, os, sys
import typer

from app.common.logging_config import setup_logging

app = typer.Typer(no_args_is_help=True)

def _get_router():
    # force native backend for these checks (uses your .env creds)
    os.environ.setdefault("ROUTER_BACKEND", "native")
    from app.infra.mt5_router import Mt5NativeRouter
    return Mt5NativeRouter()

@app.command()
def check(symbol: str = typer.Option("XAUUSD", "--symbol", "-s"),
          log_level: str = typer.Option("INFO", "--log-level", "-l")):
    """Check MT5 connectivity and print account/symbol/tick info."""
    setup_logging(log_level)
    try:
        r = _get_router()
        mt5 = r.mt5
        s = r._ensure_symbol(symbol)
        acc = mt5.account_info()
        si = mt5.symbol_info(s)
        tick = mt5.symbol_info_tick(s)
        trade_mode = getattr(acc, "trade_mode", None)   # 0=DEMO, 1=CONTEST, 2=REAL
        margin_mode = getattr(acc, "margin_mode", None) # 0=NETTING, 1=EXCHANGE, 2=HEDGING
        mm_label = {0: "RETAIL_NETTING", 1: "EXCHANGE", 2: "RETAIL_HEDGING"}.get(margin_mode, str(margin_mode))
        tm_label = {0: "DEMO", 1: "CONTEST", 2: "REAL"}.get(trade_mode, str(trade_mode))

        out = {
            "account": {
                "login": getattr(acc, "login", None),
                "server": getattr(acc, "server", None),
                "currency": getattr(acc, "currency", None),
                "trade_mode": {"raw": trade_mode, "label": tm_label},
                "margin_mode": {"raw": margin_mode, "label": mm_label},
            },
            "symbol": {
                "name": s,
                "visible": getattr(si, "visible", None),
                "digits": getattr(si, "digits", None),
                "point": getattr(si, "point", None),
            },
            "tick": {
                "ask": getattr(tick, "ask", None),
                "bid": getattr(tick, "bid", None),
            }
        }

        print(json.dumps(out, indent=2))
    except Exception as e:
        print("MT5 check failed:", e)
        sys.exit(1)

@app.command()
def place_test(symbol: str = typer.Option("XAUUSD", "--symbol", "-s"),
               side: str = typer.Option("BUY", "--side"),
               volume: float = typer.Option(0.01, "--volume", "-v"),
               entry: float = typer.Option(None, "--entry"),
               sl: float = typer.Option(None, "--sl"),
               tp: float = typer.Option(None, "--tp"),
               log_level: str = typer.Option("INFO", "--log-level", "-l")):
    """Place a tiny market/limit order, then (optionally) close it immediately."""
    setup_logging(log_level)
    try:
        r = _get_router()
        mt5 = r.mt5
        s = r._ensure_symbol(symbol)

        # fabricate a single-leg "Action" object on the fly
        from app.models import Action, Leg
        leg = Leg(
            leg_id="TEST#1",
            symbol=symbol,
            side=side.upper(),
            volume=volume,
            entry=entry,   # if None, router will choose market based on current tick vs entry
            sl=sl,
            tp=tp,
            tag="MT5_TEST",
        )
        act = Action(action_id="mt5-test-open", type="OPEN", legs=[leg], source_msg_id="MT5_TEST")
        res = r.execute(act)
        print("OPEN result:", json.dumps(res.model_dump(), indent=2))

    except Exception as e:
        print("MT5 place_test failed:", e)
        sys.exit(1)

@app.command()
def diagnose(log_level: str = typer.Option("INFO", "--log-level", "-l")):
    """Print MT5 terminal permission flags related to trading."""
    setup_logging(log_level)
    try:
        r = _get_router()
        mt5 = r.mt5
        ti = mt5.terminal_info()
        acc = mt5.account_info()
        print("terminal_info:", {k: getattr(ti, k) for k in dir(ti) if not k.startswith("_")})
        print("account_info:", {
            "login": getattr(acc, "login", None),
            "server": getattr(acc, "server", None),
            "trade_mode": getattr(acc, "trade_mode", None),   # DEMO/REAL/CONTEST
            "margin_mode": getattr(acc, "margin_mode", None)  # 2 = HEDGING
        })
    except Exception as e:
        print("diagnose failed:", e)


if __name__ == "__main__":
    app()
