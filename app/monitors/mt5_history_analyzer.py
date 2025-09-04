#!/usr/bin/env python3
"""
MT5 Trade History Analyzer - Simplified Version
Uses deals to build positions and get entry comments
"""

import os
import sys
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Any
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
    """Record of a closed position"""
    position_id: int
    message_id: str
    leg: int
    symbol: str
    open_time: datetime
    close_time: datetime
    volume: float
    profit: float = 0.0
    commission: float = 0.0
    swap: float = 0.0
    entry_comment: str = ""
    
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
        if self.last_close_time is None or pos.close_time > self.last_close_time:
            self.last_close_time = pos.close_time


class MT5HistoryAnalyzer:
    """Simplified analyzer using deals to build positions"""
    
    def __init__(self, mt5_instance):
        self.mt5 = mt5_instance
        self.now = datetime.now(timezone.utc)
        self.debug_stats = {
            'total_positions': 0,
            'positions_with_comments': 0,
            'positions_without_comments': 0,
            'total_deals': 0,
            'filtered_positions': 0
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
    
    def get_positions_from_deals(self, from_date: datetime = None, to_date: datetime = None, 
                                 filter_comments: bool = True) -> Dict[int, Dict]:
        """Build position records from deals"""
        if from_date is None:
            from_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        if to_date is None:
            to_date = self.now + timedelta(days=1)
            
        from_ts = int(from_date.timestamp())
        to_ts = int(to_date.timestamp())
        
        # Get all deals in date range
        deals = self.mt5.history_deals_get(from_ts, to_ts)
        if deals is None:
            log.warning("Failed to get history deals")
            return {}
            
        self.debug_stats['total_deals'] = len(deals)
        
        # Build positions from deals
        positions = {}
        for deal in deals:
            # Skip balance/credit operations
            deal_type = getattr(deal, 'type', -1)
            if deal_type not in [0, 1]:  # DEAL_TYPE_BUY=0, DEAL_TYPE_SELL=1
                continue
                
            position_id = getattr(deal, 'position_id', 0)
            if position_id <= 0:
                continue
                
            if position_id not in positions:
                positions[position_id] = {
                    'position_id': position_id,
                    'symbol': getattr(deal, 'symbol', 'UNKNOWN'),
                    'entry_deals': [],
                    'exit_deals': [],
                    'profit': 0.0,
                    'commission': 0.0,
                    'swap': 0.0,
                    'volume': 0.0,
                    'open_time': None,
                    'close_time': None,
                    'entry_comment': None,
                    'has_valid_comment': False
                }
            
            # Categorize deal and accumulate P&L
            entry_type = getattr(deal, 'entry', -1)
            if entry_type == 0:  # DEAL_ENTRY_IN
                positions[position_id]['entry_deals'].append(deal)
                positions[position_id]['volume'] += getattr(deal, 'volume', 0.0)
                if positions[position_id]['open_time'] is None:
                    positions[position_id]['open_time'] = getattr(deal, 'time', 0)
                # Store the first entry deal's comment
                if positions[position_id]['entry_comment'] is None:
                    comment = str(getattr(deal, 'comment', ''))
                    positions[position_id]['entry_comment'] = comment
                    # Check if it matches our format
                    if '_' in comment or '#' in comment:
                        positions[position_id]['has_valid_comment'] = True
            elif entry_type == 1:  # DEAL_ENTRY_OUT
                positions[position_id]['exit_deals'].append(deal)
                positions[position_id]['close_time'] = getattr(deal, 'time', 0)
            
            # Accumulate P&L components from all deals
            positions[position_id]['profit'] += getattr(deal, 'profit', 0.0)
            positions[position_id]['commission'] += getattr(deal, 'commission', 0.0)
            positions[position_id]['swap'] += getattr(deal, 'swap', 0.0)
        
        # Filter to only closed positions
        closed_positions = {
            pid: pos for pid, pos in positions.items() 
            if len(pos['exit_deals']) > 0  # Has at least one exit deal
        }
        
        # Optionally filter to only positions with valid comments
        if filter_comments:
            filtered_positions = {
                pid: pos for pid, pos in closed_positions.items()
                if pos['has_valid_comment']
            }
            log.info(f"Found {len(closed_positions)} closed positions, {len(filtered_positions)} with valid comments")
            self.debug_stats['total_positions'] = len(closed_positions)
            self.debug_stats['filtered_positions'] = len(filtered_positions)
            return filtered_positions
        else:
            log.info(f"Found {len(closed_positions)} closed positions from {len(deals)} deals")
            self.debug_stats['total_positions'] = len(closed_positions)
            return closed_positions
    
    def analyze_positions(self, positions_dict: Dict[int, Dict]) -> List[PositionRecord]:
        """Analyze positions built from deals"""
        position_records = []
        
        for position_id, pos_data in positions_dict.items():
            # Get comment from entry deal (already stored when building positions)
            entry_comment = pos_data['entry_comment']
            
            if not entry_comment:
                log.debug(f"No entry comment for position {position_id}")
                self.debug_stats['positions_without_comments'] += 1
                continue
            
            # Parse the comment
            msg_id, leg, sym_suffix = self.parse_comment(entry_comment)
            
            # Skip if comment doesn't match our format
            if not msg_id:
                # Try simpler pattern for just message ID
                simple_match = re.search(r'\b(\d{3,})\b', entry_comment)
                if simple_match:
                    msg_id = simple_match.group(1)
                    leg = -1  # Unknown leg
                else:
                    log.debug(f"Comment doesn't match format: '{entry_comment}'")
                    continue
            
            if leg is None:
                leg = -1
                
            self.debug_stats['positions_with_comments'] += 1
            
            # Create position record
            pos = PositionRecord(
                position_id=position_id,
                message_id=msg_id,
                leg=leg,
                symbol=pos_data['symbol'],
                open_time=datetime.fromtimestamp(pos_data['open_time'], tz=timezone.utc) if pos_data['open_time'] else self.now,
                close_time=datetime.fromtimestamp(pos_data['close_time'], tz=timezone.utc) if pos_data['close_time'] else self.now,
                volume=pos_data['volume'],
                profit=pos_data['profit'],
                commission=pos_data['commission'],
                swap=pos_data['swap'],
                entry_comment=entry_comment
            )
            
            position_records.append(pos)
        
        log.info(f"Analyzed {len(position_records)} positions with valid comments")
        return position_records
    
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
                if from_date <= pos.close_time <= to_date:
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
        
        # Sort message IDs (numeric)
        sorted_msg_ids = sorted(all_msg_ids, key=lambda x: int(x) if x.isdigit() else 999999)
        
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
        print("MT5 CLOSED TRADES SUMMARY (Simplified)")
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
        print(f"  Total deals processed: {self.debug_stats['total_deals']}")
        print(f"  Closed positions found: {self.debug_stats['total_positions']}")
        print(f"  Positions with valid comments: {self.debug_stats.get('filtered_positions', self.debug_stats['positions_with_comments'])}")
        print(f"  Positions without/invalid comments: {self.debug_stats['positions_without_comments']}")
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
    show_positions: bool = typer.Option(False, "--show-positions", help="Show individual positions"),
    show_all: bool = typer.Option(False, "--show-all", help="Show ALL positions without comment filter"),
    reconcile: bool = typer.Option(False, "--reconcile", help="Show reconciliation with all positions")
):
    """Analyze closed trades from MT5 history using simplified deals approach"""
    
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
        
        # Get positions from deals
        from_date = datetime.now(timezone.utc) - timedelta(days=days_back)
        log.info(f"Fetching deals from {from_date.strftime('%Y-%m-%d')} to now...")
        
        # First get all positions for reconciliation if requested
        if reconcile or show_all:
            all_positions = analyzer.get_positions_from_deals(from_date=from_date, filter_comments=False)
            
            # Calculate totals for all closed positions
            total_all_profit = sum(p['profit'] for p in all_positions.values())
            total_all_commission = sum(p['commission'] for p in all_positions.values())
            total_all_swap = sum(p['swap'] for p in all_positions.values())
            total_all_net = total_all_profit + total_all_commission + total_all_swap
            
            print("\n" + "="*80)
            print("RECONCILIATION WITH ALL CLOSED POSITIONS")
            print("="*80)
            print(f"Total closed positions: {len(all_positions)}")
            print(f"Total profit: {total_all_profit:.2f}")
            print(f"Total commission: {total_all_commission:.2f}")
            print(f"Total swap: {total_all_swap:.2f}")
            print(f"NET TOTAL (All positions): {total_all_net:.2f}")
            print(f"Expected from MT5 report: -218.55")
            print(f"Difference: {(total_all_net - (-218.55)):.2f}")
            
            # Count positions with valid comments
            with_comments = sum(1 for p in all_positions.values() if p['has_valid_comment'])
            print(f"\nPositions with valid comments (_/#): {with_comments}")
            print(f"Positions without valid comments: {len(all_positions) - with_comments}")
            
            if show_all:
                # Show sample positions without valid comments
                print("\nSample positions WITHOUT valid comments:")
                no_comment_positions = [p for p in all_positions.values() if not p['has_valid_comment']][:10]
                for pos in no_comment_positions:
                    print(f"  Position {pos['position_id']}: comment='{pos['entry_comment']}' "
                          f"P&L={pos['profit'] + pos['commission'] + pos['swap']:.2f}")
        
        # Now get filtered positions (with valid comments only)
        positions_dict = analyzer.get_positions_from_deals(from_date=from_date, filter_comments=True)
        
        if not positions_dict:
            print("\nNo closed positions found in the specified date range.")
            return
        
        if debug:
            # Show sample of recent positions
            print("\nSample of recent closed positions:")
            recent = sorted(positions_dict.items(), 
                          key=lambda x: x[1]['close_time'] or 0, 
                          reverse=True)[:5]
            for pid, pos_data in recent:
                close_time = datetime.fromtimestamp(pos_data['close_time']) if pos_data['close_time'] else None
                print(f"  Position {pid}: {pos_data['symbol']} "
                      f"profit={pos_data['profit']:.2f} "
                      f"comment='{pos_data['entry_comment']}' "
                      f"closed={close_time.strftime('%Y-%m-%d %H:%M') if close_time else 'N/A'}")
        
        # Analyze positions (using entry deal comments already extracted)
        position_records = analyzer.analyze_positions(positions_dict)
        
        if not position_records:
            print("\nNo positions found with valid comments in our format.")
            print("Make sure your trades have comments in the format: msgid_leg:symbol")
            print("\nShowing some entry comments found:")
            sample = list(positions_dict.items())[:10]
            for pid, pos_data in sample:
                if pos_data['entry_comment']:
                    print(f"  Position {pid}: '{pos_data['entry_comment']}'")
            return
        
        # Build summaries
        summaries = analyzer.build_summaries(position_records)
        log.info(f"Analyzed {len(summaries)} unique setups")
        
        # Get period summaries
        period_summaries = analyzer.get_time_period_summaries(summaries)
        
        # Print summary
        analyzer.print_summary(period_summaries)
        
        # Show individual positions if requested
        if show_positions and position_records:
            print("\n" + "="*80)
            print("RECENT POSITIONS (Last 20)")
            print("="*80)
            
            recent = sorted(position_records, key=lambda p: p.close_time, reverse=True)[:20]
            
            for pos in recent:
                print(f"\nPosition {pos.position_id}:")
                print(f"  Setup: {pos.message_id}, Leg: {pos.leg}")
                print(f"  Symbol: {pos.symbol}, Volume: {pos.volume:.2f}")
                print(f"  P&L: {pos.total_pnl:.2f} (profit={pos.profit:.2f}, comm={pos.commission:.2f}, swap={pos.swap:.2f})")
                print(f"  Entry comment: '{pos.entry_comment}'")
                print(f"  Closed: {pos.close_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show leg breakdown if requested
        if show_legs:
            print("\n" + "="*80)
            print("LEG BREAKDOWN (All Time)")
            print("="*80)
            
            for msg_id in sorted(summaries.keys(), key=lambda x: int(x) if x.isdigit() else 999999):
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