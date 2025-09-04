#!/usr/bin/env python3
"""
MT5 Trade History Analyzer - Version 3
Uses history_orders to preserve comments and match with deals for accurate P&L
"""

import os
import sys
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import defaultdict
from dataclasses import dataclass, field
import MetaTrader5 as mt5
import typer
from tabulate import tabulate

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.common.config import Config
from app.common.logging_config import setup_logging
from app.infra.mt5_router import Mt5NativeRouter

app = typer.Typer(no_args_is_help=False)
log = logging.getLogger("mt5_history_analyzer")

# Comment parsing regex - matches patterns like "1234_1:XAUUSD" or "1234#1:XAUUSD"
COMMENT_PATTERN = re.compile(r'(?<!\d)(?P<msg>\d+)[_#](?P<leg>\d+)(?::(?P<sym>[A-Za-z0-9+._-]+))?')

@dataclass
class PositionRecord:
    """Record of a position from orders/deals"""
    position_id: int
    message_id: str
    leg: int
    symbol: str
    open_time: datetime
    close_time: Optional[datetime]
    volume: float
    profit: float = 0.0
    commission: float = 0.0
    swap: float = 0.0
    entry_comment: str = ""
    exit_comment: str = ""
    
    @property
    def total_pnl(self) -> float:
        return self.profit + self.commission + self.swap

@dataclass
class TradeSummary:
    """Summary of trades for a specific message_id"""
    message_id: str
    total_profit: float = 0.0
    trade_count: int = 0
    volume: float = 0.0
    symbols: set = field(default_factory=set)
    first_open_time: Optional[datetime] = None
    last_close_time: Optional[datetime] = None
    legs: Dict[int, float] = field(default_factory=dict)  # leg_id -> profit
    position_records: List[PositionRecord] = field(default_factory=list)
    
    def add_position(self, pos: PositionRecord):
        """Add a position record to the summary"""
        self.position_records.append(pos)
        self.total_profit += pos.total_pnl
        self.trade_count += 1
        self.volume += pos.volume
        self.symbols.add(pos.symbol)
        
        if pos.leg not in self.legs:
            self.legs[pos.leg] = 0.0
        self.legs[pos.leg] += pos.total_pnl
        
        if self.first_open_time is None or pos.open_time < self.first_open_time:
            self.first_open_time = pos.open_time
        if pos.close_time and (self.last_close_time is None or pos.close_time > self.last_close_time):
            self.last_close_time = pos.close_time


class MT5HistoryAnalyzer:
    """Analyzes MT5 trade history using orders and deals"""
    
    def __init__(self, mt5_instance):
        self.mt5 = mt5_instance
        self.now = datetime.now(timezone.utc)
        self.debug_stats = {
            'total_orders': 0,
            'total_deals': 0,
            'filled_orders': 0,
            'matched_positions': 0,
            'unmatched_positions': 0
        }
        
    def parse_comment(self, comment: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
        """Extract (message_id, leg, symbol_suffix) from MT5 comment"""
        if not comment:
            return None, None, None
            
        match = COMMENT_PATTERN.search(str(comment))
        if match:
            msg_id = match.group("msg")
            try:
                leg = int(match.group("leg"))
            except (ValueError, TypeError):
                leg = None
            sym_suffix = match.group("sym")
            return msg_id, leg, sym_suffix
        return None, None, None
    
    def get_history_orders(self, from_date: datetime = None, to_date: datetime = None) -> List[Any]:
        """Get filled orders from history which preserve our comments"""
        if from_date is None:
            from_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if to_date is None:
            to_date = self.now + timedelta(days=1)
            
        from_ts = int(from_date.timestamp())
        to_ts = int(to_date.timestamp())
        
        # Get history orders (these preserve our comments!)
        orders = self.mt5.history_orders_get(from_ts, to_ts)
        if orders is None:
            log.warning("Failed to get history orders")
            return []
            
        # Filter for filled orders only
        filled_orders = [o for o in orders if getattr(o, 'state', 0) == 4]  # ORDER_STATE_FILLED = 4
        
        log.info(f"Retrieved {len(orders)} total orders, {len(filled_orders)} filled")
        self.debug_stats['total_orders'] = len(orders)
        self.debug_stats['filled_orders'] = len(filled_orders)
        
        return filled_orders
    
    def get_history_deals(self, from_date: datetime = None, to_date: datetime = None) -> Dict[int, List[Any]]:
        """Get deals and organize by position_id"""
        if from_date is None:
            from_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if to_date is None:
            to_date = self.now + timedelta(days=1)
            
        from_ts = int(from_date.timestamp())
        to_ts = int(to_date.timestamp())
        
        deals = self.mt5.history_deals_get(from_ts, to_ts)
        if deals is None:
            log.warning("Failed to get history deals")
            return {}
            
        self.debug_stats['total_deals'] = len(deals)
        
        # Organize deals by position_id
        deals_by_position = defaultdict(list)
        for deal in deals:
            # Skip balance operations
            deal_type = getattr(deal, 'type', -1)
            if deal_type not in [0, 1]:  # DEAL_TYPE_BUY=0, DEAL_TYPE_SELL=1
                continue
                
            position_id = getattr(deal, 'position_id', 0)
            if position_id > 0:
                deals_by_position[position_id].append(deal)
        
        log.info(f"Found {len(deals_by_position)} positions from deals")
        return deals_by_position
    
    def analyze_with_orders(self, orders: List[Any], deals_by_position: Dict[int, List[Any]]) -> List[PositionRecord]:
        """Analyze using orders (which have intact comments) matched with deals for P&L"""
        positions = []
        position_map = {}  # position_id -> PositionRecord (to avoid duplicates)
        
        # First, process all orders
        for order in orders:
            # Get order details
            position_id = getattr(order, 'position_id', 0)
            if position_id <= 0:
                continue
                
            # Skip if we already processed this position
            if position_id in position_map:
                continue
                
            # Parse comment from order (this should have our format)
            comment = str(getattr(order, 'comment', ''))
            msg_id, leg, sym_suffix = self.parse_comment(comment)
            
            # Skip orders that look like TP comments (broker overrides)
            if comment.startswith('[tp ') or comment.startswith('[sl '):
                continue
            
            if not msg_id:
                # Try simpler pattern
                simple_match = re.search(r'\b(\d{3,})\b', comment)
                if simple_match:
                    msg_id = simple_match.group(1)
            
            if not msg_id:
                continue  # Skip orders without valid message IDs
            
            if leg is None:
                leg = -1
            
            # Get deals for this position
            position_deals = deals_by_position.get(position_id, [])
            if not position_deals:
                continue  # Skip if no deals found
            
            # Calculate P&L from deals
            total_profit = 0.0
            total_commission = 0.0
            total_swap = 0.0
            entry_volume = 0.0
            
            entry_deals = []
            exit_deals = []
            
            for deal in position_deals:
                if getattr(deal, 'entry', -1) == 0:  # DEAL_ENTRY_IN
                    entry_deals.append(deal)
                    entry_volume += getattr(deal, 'volume', 0.0)
                elif getattr(deal, 'entry', -1) == 1:  # DEAL_ENTRY_OUT
                    exit_deals.append(deal)
                
                # Sum P&L components
                total_profit += getattr(deal, 'profit', 0.0)
                total_commission += getattr(deal, 'commission', 0.0)
                total_swap += getattr(deal, 'swap', 0.0)
            
            # Skip positions without exits (still open)
            if not exit_deals:
                continue
            
            # Create position record
            pos = PositionRecord(
                position_id=position_id,
                message_id=msg_id,
                leg=leg,
                symbol=getattr(order, 'symbol', 'UNKNOWN'),
                open_time=datetime.fromtimestamp(getattr(order, 'time_done', 0), tz=timezone.utc),
                close_time=datetime.fromtimestamp(getattr(exit_deals[-1], 'time', 0), tz=timezone.utc) if exit_deals else None,
                volume=entry_volume,
                profit=total_profit,
                commission=total_commission,
                swap=total_swap,
                entry_comment=comment,
                exit_comment=getattr(exit_deals[-1], 'comment', '') if exit_deals else ""
            )
            
            position_map[position_id] = pos
            
        
        # Now handle any positions we found in deals but not in orders/position_map
        # (This catches positions that might not have orders in history for some reason)
        for position_id, position_deals in deals_by_position.items():
            if position_id in position_map:
                continue  # Already processed
                
            # Try to extract info from entry deals
            entry_deals = [d for d in position_deals if getattr(d, 'entry', -1) == 0]
            exit_deals = [d for d in position_deals if getattr(d, 'entry', -1) == 1]
            
            if not entry_deals or not exit_deals:
                continue  # Skip incomplete positions
            
            # Try to find comment from entry deal
            entry_deal = entry_deals[0]
            comment = str(getattr(entry_deal, 'comment', ''))
            msg_id, leg, _ = self.parse_comment(comment)
            
            if not msg_id:
                # Skip if no valid message ID in deals either
                continue
            
            if leg is None:
                leg = -1
            
            # Calculate P&L
            total_profit = sum(getattr(d, 'profit', 0.0) for d in position_deals)
            total_commission = sum(getattr(d, 'commission', 0.0) for d in position_deals)
            total_swap = sum(getattr(d, 'swap', 0.0) for d in position_deals)
            entry_volume = sum(getattr(d, 'volume', 0.0) for d in entry_deals)
            
            pos = PositionRecord(
                position_id=position_id,
                message_id=msg_id,
                leg=leg,
                symbol=getattr(entry_deal, 'symbol', 'UNKNOWN'),
                open_time=datetime.fromtimestamp(getattr(entry_deal, 'time', 0), tz=timezone.utc),
                close_time=datetime.fromtimestamp(getattr(exit_deals[-1], 'time', 0), tz=timezone.utc),
                volume=entry_volume,
                profit=total_profit,
                commission=total_commission,
                swap=total_swap,
                entry_comment=comment,
                exit_comment=getattr(exit_deals[-1], 'comment', '')
            )
            
            position_map[position_id] = pos
            self.debug_stats['unmatched_positions'] += 1
        
        # Convert position_map to list
        positions = list(position_map.values())
        self.debug_stats['matched_positions'] = len(positions)
        
        return positions
    
    def build_summaries(self, positions: List[PositionRecord]) -> Dict[str, TradeSummary]:
        """Build summaries from position records"""
        summaries = {}
        
        for pos in positions:
            if pos.message_id not in summaries:
                summaries[pos.message_id] = TradeSummary(message_id=pos.message_id)
            summaries[pos.message_id].add_position(pos)
        
        return summaries
    
    def filter_by_date_range(self, summaries: Dict[str, TradeSummary], 
                            from_date: datetime, to_date: datetime) -> Dict[str, TradeSummary]:
        """Filter summaries by close date range"""
        filtered = {}
        for msg_id, summary in summaries.items():
            # Create a filtered summary with only positions in date range
            filtered_summary = TradeSummary(message_id=msg_id)
            
            for pos in summary.position_records:
                if pos.close_time and from_date <= pos.close_time <= to_date:
                    filtered_summary.add_position(pos)
            
            if filtered_summary.trade_count > 0:
                filtered[msg_id] = filtered_summary
                
        return filtered
    
    def get_time_period_summaries(self, summaries: Dict[str, TradeSummary]) -> Dict[str, Dict[str, TradeSummary]]:
        """Get summaries for different time periods based on CLOSE time"""
        periods = {}
        
        # Use local time to match MT5 display
        local_now = datetime.now()
        local_today_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        local_today_end = local_now
        
        # Convert to UTC for comparison
        utc_offset = local_now.astimezone().utcoffset()
        today_start_utc = (local_today_start - utc_offset).replace(tzinfo=timezone.utc)
        today_end_utc = (local_today_end - utc_offset).replace(tzinfo=timezone.utc)
        
        periods['today'] = self.filter_by_date_range(summaries, today_start_utc, today_end_utc)
        
        # Week to date (starting Sunday)
        days_since_sunday = (local_now.weekday() + 1) % 7
        local_week_start = (local_now - timedelta(days=days_since_sunday)).replace(hour=0, minute=0, second=0, microsecond=0)
        week_start_utc = (local_week_start - utc_offset).replace(tzinfo=timezone.utc)
        periods['week_to_date'] = self.filter_by_date_range(summaries, week_start_utc, today_end_utc)
        
        # Month to date
        local_month_start = local_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_start_utc = (local_month_start - utc_offset).replace(tzinfo=timezone.utc)
        periods['month_to_date'] = self.filter_by_date_range(summaries, month_start_utc, today_end_utc)
        
        # All time
        periods['all_time'] = summaries
        
        return periods
    
    def create_summary_table(self, period_summaries: Dict[str, Dict[str, TradeSummary]]) -> List[List]:
        """Create a summary table for display"""
        # Collect all unique message IDs
        all_msg_ids = set()
        for summaries in period_summaries.values():
            all_msg_ids.update(summaries.keys())
        
        # Sort message IDs (UNKNOWN last, then numeric)
        sorted_msg_ids = sorted(all_msg_ids, key=lambda x: (x == "UNKNOWN", int(x) if x != "UNKNOWN" and x.isdigit() else 999999))
        
        # Build table rows
        rows = []
        for msg_id in sorted_msg_ids:
            row = [msg_id]
            
            # Add data for each period
            for period in ['all_time', 'month_to_date', 'week_to_date', 'today']:
                summary = period_summaries[period].get(msg_id)
                if summary:
                    row.extend([
                        summary.trade_count,
                        f"{summary.total_profit:.2f}"
                    ])
                else:
                    row.extend([0, "0.00"])
            
            # Add symbols for context
            if msg_id in period_summaries['all_time']:
                symbols = ", ".join(sorted(period_summaries['all_time'][msg_id].symbols))
                row.append(symbols[:30])  # Truncate if too long
            else:
                row.append("")
            
            rows.append(row)
        
        # Add totals row
        totals = ["TOTAL"]
        for period in ['all_time', 'month_to_date', 'week_to_date', 'today']:
            total_count = sum(s.trade_count for s in period_summaries[period].values())
            total_profit = sum(s.total_profit for s in period_summaries[period].values())
            totals.extend([total_count, f"{total_profit:.2f}"])
        totals.append("")
        rows.append(totals)
        
        return rows
    
    def print_summary(self, period_summaries: Dict[str, Dict[str, TradeSummary]]):
        """Print formatted summary"""
        headers = [
            "Setup ID",
            "All Count", "All P&L",
            "MTD Count", "MTD P&L",
            "WTD Count", "WTD P&L",
            "Today Count", "Today P&L",
            "Symbols"
        ]
        
        rows = self.create_summary_table(period_summaries)
        
        print("\n" + "="*80)
        print("MT5 CLOSED TRADES SUMMARY")
        print("="*80)
        print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S local time')}")
        print(f"Today: {datetime.now().strftime('%Y-%m-%d')}")
        
        # Calculate week start
        days_since_sunday = (datetime.now().weekday() + 1) % 7
        week_start = (datetime.now() - timedelta(days=days_since_sunday)).strftime('%Y-%m-%d')
        print(f"Week Start (Sunday): {week_start}")
        print(f"Month Start: {datetime.now().strftime('%Y-%m-01')}")
        print()
        
        # Print debug stats
        print("Processing Stats:")
        print(f"  Total orders: {self.debug_stats['total_orders']}")
        print(f"  Filled orders: {self.debug_stats['filled_orders']}")
        print(f"  Total deals: {self.debug_stats['total_deals']}")
        print(f"  Matched positions: {self.debug_stats['matched_positions']}")
        print(f"  Unmatched positions (from deals): {self.debug_stats['unmatched_positions']}")
        print()
        
        # Print table
        print(tabulate(rows, headers=headers, tablefmt="grid", floatfmt=".2f"))
        
        # Print period summaries
        print("\n" + "-"*80)
        print("PERIOD SUMMARIES")
        print("-"*80)
        
        for period_name, period_label in [
            ('today', 'Today'),
            ('week_to_date', 'Week to Date'),
            ('month_to_date', 'Month to Date'),
            ('all_time', 'All Time')
        ]:
            summaries = period_summaries[period_name]
            total_profit = sum(s.total_profit for s in summaries.values())
            total_trades = sum(s.trade_count for s in summaries.values())
            setup_count = len(summaries)
            
            print(f"\n{period_label}:")
            print(f"  Setups: {setup_count}")
            print(f"  Closed Positions: {total_trades}")
            print(f"  Total P&L: {total_profit:.2f}")
            if total_trades > 0:
                print(f"  Avg per Trade: {total_profit/total_trades:.2f}")


@app.command()
def run(
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
    days_back: int = typer.Option(365, "--days", "-d", help="Number of days to analyze"),
    export_csv: Optional[str] = typer.Option(None, "--csv", help="Export to CSV file"),
    show_legs: bool = typer.Option(False, "--show-legs", help="Show breakdown by legs"),
    debug: bool = typer.Option(False, "--debug", help="Show debug information"),
    show_positions: bool = typer.Option(False, "--show-positions", help="Show individual positions")
):
    """Analyze closed trades from MT5 history using orders to preserve comments"""
    
    # Setup logging
    if debug:
        log_level = "DEBUG"
    setup_logging(log_level)
    
    # Initialize config and router
    cfg = Config()
    os.environ.setdefault("ROUTER_BACKEND", "native")
    
    try:
        # Connect to MT5
        router = Mt5NativeRouter(cfg)
        mt5_instance = router.mt5
        
        # Get account info for context
        account_info = mt5_instance.account_info()
        if account_info:
            print(f"\nAccount: {account_info.login}")
            print(f"Server: {account_info.server}")
            print(f"Balance: {account_info.balance:.2f}")
            print(f"Equity: {account_info.equity:.2f}")
            print(f"Current P&L: {account_info.profit:.2f}")
        
        # Create analyzer
        analyzer = MT5HistoryAnalyzer(mt5_instance)
        
        # Get data from MT5
        from_date = datetime.now(timezone.utc) - timedelta(days=days_back)
        log.info(f"Fetching history from {from_date.strftime('%Y-%m-%d')} to now...")
        
        # Get orders (preserves comments) and deals (has P&L)
        orders = analyzer.get_history_orders(from_date=from_date)
        deals_by_position = analyzer.get_history_deals(from_date=from_date)
        
        if debug:
            # Show sample of recent orders with comments
            print("\nSample of recent filled orders with comments:")
            recent_orders = sorted(orders, key=lambda o: getattr(o, 'time_done', 0), reverse=True)[:10]
            for order in recent_orders:
                comment = getattr(order, 'comment', '')
                print(f"  Order {order.ticket}: {order.symbol} "
                      f"pos_id={order.position_id} "
                      f"state={order.state} comment='{comment}'")
        
        # Analyze using orders + deals
        positions = analyzer.analyze_with_orders(orders, deals_by_position)
        log.info(f"Found {len(positions)} closed positions")
        
        # Build summaries
        summaries = analyzer.build_summaries(positions)
        log.info(f"Analyzed {len(summaries)} unique setups")
        
        # Get period summaries
        period_summaries = analyzer.get_time_period_summaries(summaries)
        
        # Print summary
        analyzer.print_summary(period_summaries)
        
        # Show individual positions if requested
        if show_positions and summaries:
            print("\n" + "="*80)
            print("RECENT POSITIONS (Last 20)")
            print("="*80)
            
            # Get all positions and sort by close time
            all_positions = []
            for summary in summaries.values():
                all_positions.extend(summary.position_records)
            
            recent = sorted(all_positions, key=lambda p: p.close_time or p.open_time, reverse=True)[:20]
            
            for pos in recent:
                print(f"\nPosition {pos.position_id}:")
                print(f"  Setup: {pos.message_id}, Leg: {pos.leg}")
                print(f"  Symbol: {pos.symbol}, Volume: {pos.volume:.2f}")
                print(f"  P&L: {pos.total_pnl:.2f} (profit={pos.profit:.2f}, comm={pos.commission:.2f}, swap={pos.swap:.2f})")
                print(f"  Entry comment: '{pos.entry_comment}'")
                print(f"  Exit comment: '{pos.exit_comment}'")
                if pos.close_time:
                    print(f"  Closed: {pos.close_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show leg breakdown if requested
        if show_legs:
            print("\n" + "="*80)
            print("LEG BREAKDOWN (All Time)")
            print("="*80)
            
            for msg_id in sorted(summaries.keys(), key=lambda x: (x == "UNKNOWN", int(x) if x != "UNKNOWN" and x.isdigit() else 999999)):
                summary = summaries[msg_id]
                if summary.legs and len(summary.legs) > 1:  # Only show if multiple legs
                    print(f"\nSetup {msg_id} ({summary.trade_count} positions):")
                    for leg_id in sorted(summary.legs.keys()):
                        leg_profit = summary.legs[leg_id]
                        leg_count = sum(1 for p in summary.position_records if p.leg == leg_id)
                        print(f"  Leg {leg_id}: {leg_profit:.2f} ({leg_count} trades)")
                    print(f"  Total: {summary.total_profit:.2f}")
        
        # Export to CSV if requested
        if export_csv:
            import csv
            with open(export_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                headers = [
                    "Setup ID",
                    "All Count", "All P&L",
                    "MTD Count", "MTD P&L",
                    "WTD Count", "WTD P&L",
                    "Today Count", "Today P&L",
                    "Symbols"
                ]
                writer.writerow(headers)
                rows = analyzer.create_summary_table(period_summaries)
                writer.writerows(rows)
            print(f"\nExported to {export_csv}")
            
    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()