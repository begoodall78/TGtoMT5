# scripts/peek_comments.py
import MetaTrader5 as mt5

mt5.initialize()  # or mt5.initialize(path) and mt5.login(...)
orders = mt5.orders_get()
deals  = mt5.history_deals_get(0, mt5.symbol_info_tick("XAUUSD+").time)  # rough window
print("Recent open orders:", [ (o.ticket, o.symbol, o.comment) for o in (orders or []) ])
print("Recent deals:", [ (d.ticket, d.symbol, d.comment) for d in (deals or []) ][-5:])
mt5.shutdown()
