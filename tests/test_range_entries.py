"""
Test suite for the new 16-leg range entry functionality.

Save this file as: tests/test_range_entries.py
"""

import pytest
from app.processing import plan_legs, build_actions_from_message, ParseSignal, Side

class TestRangeEntries:
    """Test suite for the new 16-leg range entry functionality"""
    
    def test_buy_range_16_legs(self):
        """BUY @ 3468/3465 creates 16 legs with correct distribution"""
        ps = ParseSignal(
            side='BUY',
            entries=[3468, 3465],
            tps=[3480, 3485, 3490, None],  # None = OPEN
            sl=3450
        )
        
        entries, tps, effective = plan_legs(ps, 5)
        
        # Should create 16 legs
        assert effective == 16
        assert len(entries) == 16
        assert len(tps) == 16
        
        # Check entry distribution (worst to best)
        expected_entries = [
            3468.00, 3468.00, 3468.00, 3468.00,  # Legs 1-4 (worst)
            3467.00, 3467.00, 3467.00, 3467.00,  # Legs 5-8
            3466.00, 3466.00, 3466.00, 3466.00,  # Legs 9-12
            3465.00, 3465.00, 3465.00, 3465.00,  # Legs 13-16 (best)
        ]
        assert entries == expected_entries
        
        # Check TP pattern repeats
        expected_tp_pattern = [3480, 3485, 3490, None] * 4
        assert tps == expected_tp_pattern
    
    def test_sell_range_16_legs(self):
        """SELL @ 3465/3468 creates 16 legs with correct distribution"""
        ps = ParseSignal(
            side='SELL',
            entries=[3465, 3468],
            tps=[3461, 3457, 3452, None],
            sl=3475
        )
        
        entries, tps, effective = plan_legs(ps, 5)
        
        assert effective == 16
        
        # Check entry distribution for SELL (worst to best)
        expected_entries = [
            3465.00, 3465.00, 3465.00, 3465.00,  # Legs 1-4 (worst)
            3466.00, 3466.00, 3466.00, 3466.00,  # Legs 5-8
            3467.00, 3467.00, 3467.00, 3467.00,  # Legs 9-12
            3468.00, 3468.00, 3468.00, 3468.00,  # Legs 13-16 (best)
        ]
        assert entries == expected_entries
    
    def test_invalid_buy_range(self):
        """BUY @ 3465/3468 raises error (wrong order)"""
        ps = ParseSignal(
            side='BUY',
            entries=[3465, 3468],  # Wrong order for BUY
            tps=[3480, 3485, 3490],
            sl=3450
        )
        
        with pytest.raises(ValueError) as exc_info:
            plan_legs(ps, 5)
        
        assert "Invalid BUY range" in str(exc_info.value)
        assert "3465/3468" in str(exc_info.value)
    
    def test_invalid_sell_range(self):
        """SELL @ 3468/3465 raises error (wrong order)"""
        ps = ParseSignal(
            side='SELL',
            entries=[3468, 3465],  # Wrong order for SELL
            tps=[3461, 3457, 3452],
            sl=3475
        )
        
        with pytest.raises(ValueError) as exc_info:
            plan_legs(ps, 5)
        
        assert "Invalid SELL range" in str(exc_info.value)
        assert "3468/3465" in str(exc_info.value)
    
    def test_single_price_unchanged(self):
        """BUY @ 3468 still creates 4 legs only"""
        ps = ParseSignal(
            side='BUY',
            entries=[3468],  # Single price
            tps=[3480, 3485, 3490, None],
            sl=3450
        )
        
        entries, tps, effective = plan_legs(ps, 5)
        
        # Should still be 4 legs for single price
        assert effective == 4
        assert len(entries) == 4
        assert all(e == 3468 for e in entries)
    
    def test_rounding_to_2dp(self):
        """Test entries are rounded to 2 decimal places"""
        ps = ParseSignal(
            side='BUY',
            entries=[3468.50, 3465.25],  # 3.25 range / 3 = 1.0833...
            tps=[3480, 3485, 3490, None],
            sl=3450
        )
        
        entries, tps, effective = plan_legs(ps, 5)
        
        # Check rounding
        expected_entries = [
            3468.50, 3468.50, 3468.50, 3468.50,  # Legs 1-4
            3467.42, 3467.42, 3467.42, 3467.42,  # Legs 5-8 (3468.50 - 1.0833 = 3467.4167 -> 3467.42)
            3466.33, 3466.33, 3466.33, 3466.33,  # Legs 9-12 (3468.50 - 2.1666 = 3466.3334 -> 3466.33)
            3465.25, 3465.25, 3465.25, 3465.25,  # Legs 13-16
        ]
        assert entries == expected_entries
    
    def test_tp_distribution_16_legs(self):
        """TPs repeat pattern across all 16 legs"""
        ps = ParseSignal(
            side='BUY',
            entries=[3468, 3465],
            tps=[3480, 3485, 3490, None],  # 4 TPs including OPEN
            sl=3450
        )
        
        entries, tps, effective = plan_legs(ps, 5)
        
        # Each group of 4 legs should have the same TP pattern
        for i in range(0, 16, 4):
            group_tps = tps[i:i+4]
            assert group_tps == [3480, 3485, 3490, None]
    
    def test_full_message_processing(self):
        """Test complete message processing with 16 legs"""
        text = """BUY @ 3468/3465
TP 3480
TP 3485
TP 3490
TP OPEN
SL 3450"""
        
        actions = build_actions_from_message("test_msg", text)
        
        assert len(actions) == 1
        action = actions[0]
        assert action.type == 'OPEN'
        assert len(action.legs) == 16
        
        # Check first leg (worst price)
        assert action.legs[0].entry == 3468.00
        assert action.legs[0].tp == 3480
        assert action.legs[0].sl == 3450
        
        # Check last leg (best price)
        assert action.legs[15].entry == 3465.00
        assert action.legs[15].tp == None  # OPEN
        assert action.legs[15].sl == 3450
    
    def test_invalid_range_full_message(self):
        """Test invalid range is caught in full message processing"""
        text = """BUY @ 3465/3468
                    TP 3480
                    TP 3485
                    SL 3450"""
        
        # Should return empty list for invalid range
        actions = build_actions_from_message("test_msg", text)
        assert actions == []
    
    def test_small_range(self):
        """Test very small price range"""
        ps = ParseSignal(
            side='BUY',
            entries=[3468.10, 3468.00],  # 0.10 range
            tps=[3480],
            sl=3450
        )
        
        entries, tps, effective = plan_legs(ps, 5)
        
        # Should still work with small ranges
        expected_entries = [
            3468.10, 3468.10, 3468.10, 3468.10,  # Legs 1-4
            3468.07, 3468.07, 3468.07, 3468.07,  # Legs 5-8 (step = 0.0333)
            3468.03, 3468.03, 3468.03, 3468.03,  # Legs 9-12
            3468.00, 3468.00, 3468.00, 3468.00,  # Legs 13-16
        ]
        assert entries == expected_entries