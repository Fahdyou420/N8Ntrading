"""
Test suite for Forex Trading Automation Stack
Run with: pytest test_trading_stack.py -v
"""

import pytest
import json
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

# Import our main application
from main import app, compute_volume, compute_pip_value, enforce_risk_limits
from main import generate_signal_soros_macro_breakout, generate_signal_jones_trend
from main import STRATEGY_FUNCTIONS, MT5Manager

# Test client
client = TestClient(app)

# Test API key for testing
TEST_API_KEY = "test_key_123"

class TestRiskManagement:
    """Test risk management functions"""
    
    def test_compute_pip_value(self):
        """Test pip value calculation for different currency pairs"""
        # Standard pairs
        assert compute_pip_value("EURUSD") == 0.0001
        assert compute_pip_value("GBPUSD") == 0.0001
        assert compute_pip_value("AUDUSD") == 0.0001
        
        # JPY pairs
        assert compute_pip_value("USDJPY") == 0.01
        assert compute_pip_value("EURJPY") == 0.01
        assert compute_pip_value("GBPJPY") == 0.01
    
    def test_compute_volume(self):
        """Test position size calculation"""
        # Test with EURUSD
        volume = compute_volume(
            account_balance=10000,
            risk_pct=2.0,
            sl_pips=50,
            symbol="EURUSD"
        )
        assert 0.01 <= volume <= 10.0
        assert isinstance(volume, float)
        
        # Test with JPY pair
        volume_jpy = compute_volume(
            account_balance=10000,
            risk_pct=1.0,
            sl_pips=20,
            symbol="USDJPY"
        )
        assert 0.01 <= volume_jpy <= 10.0
        
        # Test edge cases
        volume_min = compute_volume(10, 0.1, 100, "EURUSD")
        assert volume_min >= 0.01  # Minimum volume
        
        volume_max = compute_volume(1000000, 10, 1, "EURUSD")
        assert volume_max <= 10.0  # Maximum volume cap
    
    def test_enforce_risk_limits(self):
        """Test risk limit enforcement"""
        # Normal conditions
        allowed, reason = enforce_risk_limits(open_positions=2, daily_pnl=1.5)
        assert allowed is True
        assert "passed" in reason.lower()
        
        # Too many positions
        allowed, reason = enforce_risk_limits(open_positions=10, daily_pnl=0)
        assert allowed is False
        assert "positions" in reason.lower()
        
        # Daily loss limit exceeded
        allowed, reason = enforce_risk_limits(open_positions=1, daily_pnl=-15)
        assert allowed is False
        assert "loss" in reason.lower()


class TestSignalGeneration:
    """Test signal generation functions"""
    
    def test_soros_strategy(self):
        """Test Soros macro breakout strategy"""
        signal = generate_signal_soros_macro_breakout("EURUSD")
        
        assert signal.strategy == "soros_macro_breakout"
        assert signal.symbol == "EURUSD"
        assert signal.direction in ["BUY", "SELL"]
        assert signal.confidence >= 0 and signal.confidence <= 1
        assert signal.sl_pips > 0
        assert signal.tp_pips > 0
        assert signal.suggested_volume_lots > 0
        assert signal.signal_id is not None
        assert signal.timestamp is not None
        assert signal.reason is not None
    
    def test_jones_strategy(self):
        """Test Paul Tudor Jones trend strategy"""
        signal = generate_signal_jones_trend("GBPUSD")
        
        assert signal.strategy == "jones_trend"
        assert signal.symbol == "GBPUSD"
        assert signal.direction in ["BUY", "SELL"]
        assert signal.confidence >= 0 and signal.confidence <= 1
        assert signal.suggested_volume_lots >= 0  # Can be 0 for no signal
        assert signal.signal_id is not None
    
    def test_all_strategies_available(self):
        """Test that all required strategies are implemented"""
        required_strategies = [
            "soros_macro_breakout",
            "jones_trend", 
            "simons_stat_arb",
            "druckenmiller_macro",
            "burry_carry"
        ]
        
        for strategy in required_strategies:
            assert strategy in STRATEGY_FUNCTIONS
            
        # Test each strategy generates valid signals
        for strategy_name, strategy_func in STRATEGY_FUNCTIONS.items():
            signal = strategy_func("EURUSD")
            assert signal.strategy == strategy_name
            assert signal.symbol == "EURUSD"


class TestMT5Manager:
    """Test MetaTrader 5 manager functionality"""
    
    def test_paper_mode_connection(self):
        """Test MT5 connection in paper mode"""
        mt5_mgr = MT5Manager()
        with patch.dict('os.environ', {'PAPER_MODE': 'true'}):
            result = mt5_mgr.connect()
            assert result is True
            assert mt5_mgr.connected is True
    
    def test_get_symbol_info_paper_mode(self):
        """Test symbol info retrieval in paper mode"""
        mt5_mgr = MT5Manager()
        mt5_mgr.connected = True
        
        with patch.dict('os.environ', {'PAPER_MODE': 'true'}):
            info = mt5_mgr.get_symbol_info("EURUSD")
            assert info is not None
            assert "bid" in info
            assert "ask" in info
            assert "visible" in info
            assert info["visible"] is True
    
    @patch('main.mt5')
    def test_send_order_paper_mode(self, mock_mt5):
        """Test order sending in paper mode"""
        from main import OrderRequest
        
        mt5_mgr = MT5Manager()
        mt5_mgr.connected = True
        
        order_request = OrderRequest(
            symbol="EURUSD",
            direction="BUY",
            volume=0.1,
            sl=1.0400,
            tp=1.0600,
            comment="test_order",
            idempotency_key="test-123"
        )
        
        with patch.dict('os.environ', {'PAPER_MODE': 'true'}):
            result = mt5_mgr.send_order(order_request)
            assert result.success is True
            assert result.order_id is not None
            assert result.executed_price is not None


class TestAPIEndpoints:
    """Test FastAPI endpoints"""
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_health_endpoint(self):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "mt5_connected" in data
        assert "paper_mode" in data
        assert "timestamp" in data
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_generate_signal_endpoint(self):
        """Test single signal generation endpoint"""
        headers = {"X-API-KEY": TEST_API_KEY}
        payload = {
            "strategy": "soros_macro_breakout",
            "symbol": "EURUSD",
            "timeframe": "M5"
        }
        
        response = client.post("/generate", json=payload, headers=headers)
        assert response.status_code == 200
        
        data = response.json()
        assert "signal" in data
        signal = data["signal"]
        assert signal["strategy"] == "soros_macro_breakout"
        assert signal["symbol"] == "EURUSD"
        assert signal["direction"] in ["BUY", "SELL"]
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_batch_generate_endpoint(self):
        """Test batch signal generation endpoint"""
        headers = {"X-API-KEY": TEST_API_KEY}
        payload = {
            "strategies": ["soros_macro_breakout", "jones_trend"],
            "symbols": ["EURUSD", "GBPUSD"]
        }
        
        response = client.post("/batch_generate", json=payload, headers=headers)
        assert response.status_code == 200
        
        data = response.json()
        assert "signals" in data
        assert "count" in data
        assert len(data["signals"]) == 4  # 2 strategies × 2 symbols
    
    def test_api_key_required(self):
        """Test that API key is required for protected endpoints"""
        payload = {"strategy": "soros_macro_breakout", "symbol": "EURUSD"}
        
        # No API key
        response = client.post("/generate", json=payload)
        assert response.status_code == 422
        
        # Wrong API key
        headers = {"X-API-KEY": "wrong_key"}
        response = client.post("/generate", json=payload, headers=headers)
        assert response.status_code == 401
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_order_endpoint(self):
        """Test order execution endpoint"""
        headers = {"X-API-KEY": TEST_API_KEY}
        payload = {
            "symbol": "EURUSD",
            "direction": "BUY",
            "volume": 0.1,
            "sl": 1.0400,
            "tp": 1.0600,
            "comment": "test_order",
            "idempotency_key": "test-order-123"
        }
        
        with patch.dict('os.environ', {'PAPER_MODE': 'true'}):
            response = client.post("/order", json=payload, headers=headers)
            assert response.status_code == 200
            
            data = response.json()
            assert "success" in data
            assert data["success"] is True
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_webhook_endpoints(self):
        """Test N8N webhook endpoints"""
        # Signal webhook
        response = client.post("/webhook/signal", json={"test": "data"})
        assert response.status_code == 200
        assert "status" in response.json()
        
        # Approval webhook
        response = client.post("/webhook/approval", json={"action": "approve"})
        assert response.status_code == 200
        assert "status" in response.json()


class TestDataValidation:
    """Test data validation and error handling"""
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_invalid_strategy(self):
        """Test handling of invalid strategy names"""
        headers = {"X-API-KEY": TEST_API_KEY}
        payload = {
            "strategy": "invalid_strategy",
            "symbol": "EURUSD"
        }
        
        response = client.post("/generate", json=payload, headers=headers)
        assert response.status_code == 400
        assert "Unknown strategy" in response.json()["detail"]
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_invalid_direction(self):
        """Test handling of invalid order direction"""
        headers = {"X-API-KEY": TEST_API_KEY}
        payload = {
            "symbol": "EURUSD",
            "direction": "INVALID",
            "volume": 0.1,
            "comment": "test",
            "idempotency_key": "test-123"
        }
        
        response = client.post("/order", json=payload, headers=headers)
        assert response.status_code == 422  # Pydantic validation error
    
    def test_signal_data_structure(self):
        """Test that signals have all required fields"""
        signal = generate_signal_soros_macro_breakout("EURUSD")
        
        required_fields = [
            "signal_id", "strategy", "symbol", "direction", "entry_price",
            "sl", "tp", "sl_pips", "tp_pips", "suggested_volume_lots",
            "confidence", "timestamp", "reason"
        ]
        
        for field in required_fields:
            assert hasattr(signal, field), f"Signal missing required field: {field}"
            assert getattr(signal, field) is not None, f"Signal field {field} is None"


class TestErrorHandling:
    """Test error handling scenarios"""
    
    @patch('main.mt5_manager')
    def test_mt5_connection_failure(self, mock_mt5_manager):
        """Test handling of MT5 connection failures"""
        mock_mt5_manager.connect.return_value = False
        mock_mt5_manager.connected = False
        
        # Should still work in paper mode
        with patch.dict('os.environ', {'PAPER_MODE': 'true', 'MT5_REST_API_KEY': TEST_API_KEY}):
            headers = {"X-API-KEY": TEST_API_KEY}
            response = client.get("/health", headers=headers)
            assert response.status_code == 200
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_zero_volume_handling(self):
        """Test handling of zero volume orders"""
        headers = {"X-API-KEY": TEST_API_KEY}
        payload = {
            "symbol": "EURUSD",
            "direction": "BUY",
            "volume": 0.0,  # Invalid volume
            "comment": "test",
            "idempotency_key": "test-zero-vol"
        }
        
        response = client.post("/order", json=payload, headers=headers)
        # Should either reject or normalize to minimum volume
        assert response.status_code in [200, 400]


class TestIntegration:
    """Integration tests"""
    
    @patch.dict('os.environ', {
        'MT5_REST_API_KEY': TEST_API_KEY,
        'PAPER_MODE': 'true',
        'MANUAL_APPROVAL': 'false'
    })
    def test_full_signal_to_order_flow(self):
        """Test complete flow from signal generation to order execution"""
        headers = {"X-API-KEY": TEST_API_KEY}
        
        # 1. Generate signal
        signal_payload = {
            "strategy": "soros_macro_breakout",
            "symbol": "EURUSD"
        }
        
        signal_response = client.post("/generate", json=signal_payload, headers=headers)
        assert signal_response.status_code == 200
        
        signal_data = signal_response.json()["signal"]
        
        # 2. Execute order based on signal
        if signal_data["suggested_volume_lots"] > 0:  # Only if valid signal
            order_payload = {
                "symbol": signal_data["symbol"],
                "direction": signal_data["direction"],
                "volume": signal_data["suggested_volume_lots"],
                "sl": signal_data["sl"],
                "tp": signal_data["tp"],
                "comment": signal_data["strategy"],
                "idempotency_key": signal_data["signal_id"]
            }
            
            order_response = client.post("/order", json=order_payload, headers=headers)
            assert order_response.status_code == 200
            
            order_data = order_response.json()
            assert order_data["success"] is True
    
    @patch.dict('os.environ', {'MT5_REST_API_KEY': TEST_API_KEY})
    def test_idempotency(self):
        """Test order idempotency"""
        headers = {"X-API-KEY": TEST_API_KEY}
        payload = {
            "symbol": "EURUSD",
            "direction": "BUY",
            "volume": 0.1,
            "comment": "idempotency_test",
            "idempotency_key": "unique-test-123"
        }
        
        with patch.dict('os.environ', {'PAPER_MODE': 'true'}):
            # First request
            response1 = client.post("/order", json=payload, headers=headers)
            assert response1.status_code == 200
            
            # Second request with same idempotency key should succeed
            # (In production, this would check database for duplicates)
            response2 = client.post("/order", json=payload, headers=headers)
            assert response2.status_code == 200


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])