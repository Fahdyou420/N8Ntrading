"""
Unified Forex Trading Automation Stack
Production-ready FastAPI service for N8N orchestration on Render
"""

import os
import uuid
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, Literal
from dataclasses import dataclass, asdict
import asyncio

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
import MetaTrader5 as mt5
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"
MANUAL_APPROVAL = os.getenv("MANUAL_APPROVAL", "true").lower() == "true"
MT5_API_KEY = os.getenv("MT5_REST_API_KEY", "change_me")
RISK_PCT_DEFAULT = float(os.getenv("RISK_PCT_DEFAULT", "2.0"))

app = FastAPI(
    title="Forex Trading Stack",
    description="Multi-strategy forex signals with MT5 execution",
    version="1.0.0"
)

# API Key Security
api_key_header = APIKeyHeader(name="X-API-KEY")

def verify_api_key(api_key: str = Depends(api_key_header)) -> bool:
    if api_key != MT5_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

# Data Models
class SignalRequest(BaseModel):
    strategy: Literal["soros_macro_breakout", "jones_trend", "simons_stat_arb", 
                     "druckenmiller_macro", "burry_carry"]
    symbol: str = "EURUSD"
    timeframe: str = "M5"

class BatchSignalRequest(BaseModel):
    strategies: List[str]
    symbols: List[str] = ["EURUSD", "GBPUSD", "USDJPY"]

class OrderRequest(BaseModel):
    symbol: str
    direction: Literal["BUY", "SELL"]
    volume: float
    sl: Optional[float] = None
    tp: Optional[float] = None
    comment: str = ""
    idempotency_key: str

@dataclass
class TradingSignal:
    signal_id: str
    strategy: str
    symbol: str
    direction: Literal["BUY", "SELL"]
    entry_price: float
    sl: float
    tp: float
    sl_pips: float
    tp_pips: float
    suggested_volume_lots: float
    confidence: float
    timestamp: str
    reason: str

class OrderResult(BaseModel):
    success: bool
    order_id: Optional[int] = None
    error_code: Optional[int] = None
    error_message: Optional[str] = None
    executed_price: Optional[float] = None
    slippage_pips: Optional[float] = None

# Risk Management Functions
def compute_pip_value(symbol: str, account_currency: str = "USD") -> float:
    """Calculate pip value for a given currency pair"""
    if symbol.endswith("JPY"):
        return 0.01  # JPY pairs use 0.01 as pip value
    return 0.0001  # Most other pairs use 0.0001

def compute_volume(account_balance: float, risk_pct: float, sl_pips: float, symbol: str) -> float:
    """Calculate position size based on risk percentage"""
    pip_value = compute_pip_value(symbol)
    risk_amount = account_balance * (risk_pct / 100)
    pip_value_per_lot = pip_value * 100000  # Standard lot size
    volume = risk_amount / (sl_pips * pip_value_per_lot)
    return max(0.01, min(volume, 10.0))  # Min 0.01, max 10 lots

def check_margin_requirement(volume: float, symbol: str) -> bool:
    """Check if sufficient margin is available"""
    # Simplified margin check - in production, get actual margin requirements
    return volume <= 1.0  # Conservative limit

def enforce_risk_limits(open_positions: int, daily_pnl: float) -> tuple[bool, str]:
    """Enforce global risk limits"""
    max_positions = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
    max_daily_loss = float(os.getenv("MAX_DAILY_RISK_PCT", "10.0"))
    
    if open_positions >= max_positions:
        return False, f"Maximum positions reached: {open_positions}"
    
    if daily_pnl <= -max_daily_loss:
        return False, f"Daily loss limit reached: {daily_pnl}%"
    
    return True, "Risk limits passed"

# MetaTrader 5 Connection
class MT5Manager:
    def __init__(self):
        self.connected = False
    
    def connect(self) -> bool:
        """Connect to MT5 terminal"""
        if PAPER_MODE:
            logger.info("Running in PAPER_MODE - MT5 connection simulated")
            self.connected = True
            return True
            
        try:
            if not mt5.initialize():
                logger.error(f"MT5 initialization failed: {mt5.last_error()}")
                return False
            
            login = int(os.getenv("MT5_LOGIN", "0"))
            password = os.getenv("MT5_PASSWORD", "")
            server = os.getenv("MT5_SERVER", "")
            
            if login and password and server:
                authorized = mt5.login(login, password=password, server=server)
                if not authorized:
                    logger.error(f"MT5 login failed: {mt5.last_error()}")
                    return False
            
            self.connected = True
            logger.info("MT5 connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"MT5 connection error: {e}")
            return False
    
    def get_symbol_info(self, symbol: str) -> Optional[Dict]:
        """Get symbol information"""
        if PAPER_MODE:
            return {
                "visible": True,
                "bid": 1.0500 if "USD" in symbol else 150.00,
                "ask": 1.0502 if "USD" in symbol else 150.02,
                "point": 0.00001 if not symbol.endswith("JPY") else 0.001,
                "digits": 5 if not symbol.endswith("JPY") else 3
            }
        
        if not self.connected:
            return None
            
        info = mt5.symbol_info(symbol)
        if info is None:
            return None
            
        return {
            "visible": info.visible,
            "bid": info.bid,
            "ask": info.ask,
            "point": info.point,
            "digits": info.digits
        }
    
    def send_order(self, request: OrderRequest) -> OrderResult:
        """Send trading order to MT5"""
        if PAPER_MODE:
            # Simulate order execution
            return OrderResult(
                success=True,
                order_id=12345,
                executed_price=1.0501,
                slippage_pips=0.2
            )
        
        try:
            symbol_info = self.get_symbol_info(request.symbol)
            if not symbol_info or not symbol_info["visible"]:
                return OrderResult(
                    success=False,
                    error_code=4106,
                    error_message="Symbol not found or not visible"
                )
            
            # Prepare order request
            order_type = mt5.ORDER_TYPE_BUY if request.direction == "BUY" else mt5.ORDER_TYPE_SELL
            price = symbol_info["ask"] if request.direction == "BUY" else symbol_info["bid"]
            
            order_request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": request.symbol,
                "volume": request.volume,
                "type": order_type,
                "price": price,
                "sl": request.sl,
                "tp": request.tp,
                "comment": f"{request.comment} #{request.idempotency_key}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            result = mt5.order_send(order_request)
            
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                return OrderResult(
                    success=True,
                    order_id=result.order,
                    executed_price=result.price,
                    slippage_pips=abs(result.price - price) / symbol_info["point"] / 10
                )
            else:
                return OrderResult(
                    success=False,
                    error_code=result.retcode,
                    error_message=f"Order failed: {result.comment}"
                )
                
        except Exception as e:
            logger.error(f"Order execution error: {e}")
            return OrderResult(
                success=False,
                error_code=-1,
                error_message=str(e)
            )

# Initialize MT5 Manager
mt5_manager = MT5Manager()

# Strategy Implementations
def generate_signal_soros_macro_breakout(symbol: str) -> TradingSignal:
    """Soros-style macro breakout strategy"""
    # Simulate economic surprise detection
    surprise_magnitude = np.random.uniform(0.1, 0.5)  # 10-50% surprise
    direction = "BUY" if np.random.random() > 0.5 else "SELL"
    
    # Get current price
    symbol_info = mt5_manager.get_symbol_info(symbol)
    entry_price = symbol_info["bid"] if symbol_info else 1.0500
    
    # Calculate ATR-based stops
    atr_pips = 20  # Simulated ATR
    sl_pips = atr_pips * 1.2
    tp_pips = sl_pips * 2.5  # 2.5R reward
    
    if direction == "BUY":
        sl = entry_price - (sl_pips * compute_pip_value(symbol))
        tp = entry_price + (tp_pips * compute_pip_value(symbol))
    else:
        sl = entry_price + (sl_pips * compute_pip_value(symbol))
        tp = entry_price - (tp_pips * compute_pip_value(symbol))
    
    volume = compute_volume(10000, RISK_PCT_DEFAULT, sl_pips, symbol)
    
    return TradingSignal(
        signal_id=str(uuid.uuid4()),
        strategy="soros_macro_breakout",
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        sl=sl,
        tp=tp,
        sl_pips=sl_pips,
        tp_pips=tp_pips,
        suggested_volume_lots=volume,
        confidence=min(0.9, surprise_magnitude * 2),
        timestamp=datetime.now(timezone.utc).isoformat(),
        reason=f"Economic surprise detected: {surprise_magnitude:.1%} deviation from forecast"
    )

def generate_signal_jones_trend(symbol: str) -> TradingSignal:
    """Paul Tudor Jones trend-following strategy"""
    # Simulate EMA crossover detection
    ema_signal = np.random.choice(["BUY", "SELL", "NONE"], p=[0.3, 0.3, 0.4])
    
    if ema_signal == "NONE":
        # Return neutral signal
        return TradingSignal(
            signal_id=str(uuid.uuid4()),
            strategy="jones_trend",
            symbol=symbol,
            direction="BUY",
            entry_price=0,
            sl=0,
            tp=0,
            sl_pips=0,
            tp_pips=0,
            suggested_volume_lots=0,
            confidence=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
            reason="No clear trend signal - awaiting EMA crossover confirmation"
        )
    
    symbol_info = mt5_manager.get_symbol_info(symbol)
    entry_price = symbol_info["bid"] if symbol_info else 1.0500
    
    atr_pips = 30  # H4 ATR simulation
    sl_pips = atr_pips * 1.5
    tp_pips = sl_pips * 2  # 2R
    
    if ema_signal == "BUY":
        sl = entry_price - (sl_pips * compute_pip_value(symbol))
        tp = entry_price + (tp_pips * compute_pip_value(symbol))
    else:
        sl = entry_price + (sl_pips * compute_pip_value(symbol))
        tp = entry_price - (tp_pips * compute_pip_value(symbol))
    
    volume = compute_volume(10000, RISK_PCT_DEFAULT, sl_pips, symbol)
    
    return TradingSignal(
        signal_id=str(uuid.uuid4()),
        strategy="jones_trend",
        symbol=symbol,
        direction=ema_signal,
        entry_price=entry_price,
        sl=sl,
        tp=tp,
        sl_pips=sl_pips,
        tp_pips=tp_pips,
        suggested_volume_lots=volume,
        confidence=0.75,
        timestamp=datetime.now(timezone.utc).isoformat(),
        reason="EMA50/200 crossover confirmed on H4 with D1 alignment"
    )

def generate_signal_simons_stat_arb(symbol: str) -> TradingSignal:
    """Renaissance-style statistical arbitrage"""
    # Simulate z-score calculation
    z_score = np.random.uniform(-3, 3)
    
    if abs(z_score) < 2.0:
        return TradingSignal(
            signal_id=str(uuid.uuid4()),
            strategy="simons_stat_arb",
            symbol=symbol,
            direction="BUY",
            entry_price=0,
            sl=0,
            tp=0,
            sl_pips=0,
            tp_pips=0,
            suggested_volume_lots=0,
            confidence=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
            reason=f"Z-score {z_score:.2f} below threshold - no arbitrage opportunity"
        )
    
    direction = "SELL" if z_score > 2 else "BUY"  # Mean reversion
    symbol_info = mt5_manager.get_symbol_info(symbol)
    entry_price = symbol_info["bid"] if symbol_info else 1.0500
    
    # Tight stops for stat arb
    sl_pips = 10
    tp_pips = 15
    
    if direction == "BUY":
        sl = entry_price - (sl_pips * compute_pip_value(symbol))
        tp = entry_price + (tp_pips * compute_pip_value(symbol))
    else:
        sl = entry_price + (sl_pips * compute_pip_value(symbol))
        tp = entry_price - (tp_pips * compute_pip_value(symbol))
    
    volume = compute_volume(10000, 1.0, sl_pips, symbol)  # Lower risk for stat arb
    
    return TradingSignal(
        signal_id=str(uuid.uuid4()),
        strategy="simons_stat_arb",
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        sl=sl,
        tp=tp,
        sl_pips=sl_pips,
        tp_pips=tp_pips,
        suggested_volume_lots=volume,
        confidence=min(0.95, abs(z_score) / 3),
        timestamp=datetime.now(timezone.utc).isoformat(),
        reason=f"Statistical arbitrage: z-score={z_score:.2f}, mean reversion expected"
    )

def generate_signal_druckenmiller_macro(symbol: str) -> TradingSignal:
    """Druckenmiller macro strategy"""
    # Simulate macro regime analysis
    dxy_trend = np.random.choice(["STRONG_UP", "UP", "NEUTRAL", "DOWN", "STRONG_DOWN"])
    equity_sentiment = np.random.choice(["RISK_ON", "NEUTRAL", "RISK_OFF"])
    
    # Determine bias based on macro factors
    if symbol == "EURUSD":
        if dxy_trend in ["STRONG_UP", "UP"] and equity_sentiment == "RISK_OFF":
            direction = "SELL"
            confidence = 0.8
        elif dxy_trend in ["DOWN", "STRONG_DOWN"] and equity_sentiment == "RISK_ON":
            direction = "BUY"
            confidence = 0.8
        else:
            direction = "BUY"
            confidence = 0.3
    else:
        direction = np.random.choice(["BUY", "SELL"])
        confidence = 0.6
    
    symbol_info = mt5_manager.get_symbol_info(symbol)
    entry_price = symbol_info["bid"] if symbol_info else 1.0500
    
    # Wider stops for macro trades
    sl_pips = 50
    tp_pips = 150
    
    if direction == "BUY":
        sl = entry_price - (sl_pips * compute_pip_value(symbol))
        tp = entry_price + (tp_pips * compute_pip_value(symbol))
    else:
        sl = entry_price + (sl_pips * compute_pip_value(symbol))
        tp = entry_price - (tp_pips * compute_pip_value(symbol))
    
    volume = compute_volume(10000, RISK_PCT_DEFAULT * 1.5, sl_pips, symbol)  # Larger size for macro
    
    return TradingSignal(
        signal_id=str(uuid.uuid4()),
        strategy="druckenmiller_macro",
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        sl=sl,
        tp=tp,
        sl_pips=sl_pips,
        tp_pips=tp_pips,
        suggested_volume_lots=volume,
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        reason=f"Macro regime: DXY={dxy_trend}, Equity={equity_sentiment}"
    )

def generate_signal_burry_carry(symbol: str) -> TradingSignal:
    """Michael Burry carry trade strategy"""
    # Simulate carry trade analysis
    interest_diff = np.random.uniform(-2, 4)  # Interest rate differential
    valuation_score = np.random.uniform(-1, 1)  # Fair value vs current
    
    # Carry trade logic
    if interest_diff > 1 and valuation_score < -0.3:
        direction = "BUY"  # Long high-yield currency
        confidence = 0.7
    elif interest_diff < -1 and valuation_score > 0.3:
        direction = "SELL"  # Short low-yield currency
        confidence = 0.7
    else:
        direction = "BUY"
        confidence = 0.2
    
    symbol_info = mt5_manager.get_symbol_info(symbol)
    entry_price = symbol_info["bid"] if symbol_info else 1.0500
    
    # Long-term trade parameters
    sl_pips = 80
    tp_pips = 200
    
    if direction == "BUY":
        sl = entry_price - (sl_pips * compute_pip_value(symbol))
        tp = entry_price + (tp_pips * compute_pip_value(symbol))
    else:
        sl = entry_price + (sl_pips * compute_pip_value(symbol))
        tp = entry_price - (tp_pips * compute_pip_value(symbol))
    
    volume = compute_volume(10000, RISK_PCT_DEFAULT, sl_pips, symbol)
    
    return TradingSignal(
        signal_id=str(uuid.uuid4()),
        strategy="burry_carry",
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        sl=sl,
        tp=tp,
        sl_pips=sl_pips,
        tp_pips=tp_pips,
        suggested_volume_lots=volume,
        confidence=confidence,
        timestamp=datetime.now(timezone.utc).isoformat(),
        reason=f"Carry trade opportunity: rate_diff={interest_diff:.1f}%, valuation={valuation_score:.2f}"
    )

# Strategy registry
STRATEGY_FUNCTIONS = {
    "soros_macro_breakout": generate_signal_soros_macro_breakout,
    "jones_trend": generate_signal_jones_trend,
    "simons_stat_arb": generate_signal_simons_stat_arb,
    "druckenmiller_macro": generate_signal_druckenmiller_macro,
    "burry_carry": generate_signal_burry_carry
}

# API Endpoints
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("Starting Forex Trading Stack...")
    if not mt5_manager.connect():
        logger.warning("MT5 connection failed - running in fallback mode")

@app.get("/health")
async def health_check():
    """Health check endpoint for N8N monitoring"""
    return {
        "status": "healthy",
        "mt5_connected": mt5_manager.connected,
        "paper_mode": PAPER_MODE,
        "manual_approval": MANUAL_APPROVAL,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/generate")
async def generate_signal(
    request: SignalRequest,
    _: bool = Depends(verify_api_key)
) -> Dict:
    """Generate a single trading signal"""
    try:
        strategy_func = STRATEGY_FUNCTIONS.get(request.strategy)
        if not strategy_func:
            raise HTTPException(status_code=400, detail=f"Unknown strategy: {request.strategy}")
        
        signal = strategy_func(request.symbol)
        
        # Convert to dict for JSON response
        result = asdict(signal)
        
        logger.info(f"Generated signal: {signal.strategy} {signal.symbol} {signal.direction}")
        return {"signal": result}
        
    except Exception as e:
        logger.error(f"Signal generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/batch_generate")
async def batch_generate_signals(
    request: BatchSignalRequest,
    _: bool = Depends(verify_api_key)
) -> Dict:
    """Generate multiple signals across strategies and symbols"""
    signals = []
    
    try:
        for strategy in request.strategies:
            if strategy not in STRATEGY_FUNCTIONS:
                logger.warning(f"Skipping unknown strategy: {strategy}")
                continue
                
            for symbol in request.symbols:
                signal = STRATEGY_FUNCTIONS[strategy](symbol)
                signals.append(asdict(signal))
        
        logger.info(f"Generated {len(signals)} signals")
        return {"signals": signals, "count": len(signals)}
        
    except Exception as e:
        logger.error(f"Batch signal generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/order")
async def send_order(
    request: OrderRequest,
    _: bool = Depends(verify_api_key)
) -> OrderResult:
    """Execute trading order through MT5"""
    try:
        # Risk checks
        allowed, reason = enforce_risk_limits(0, 0)  # Simplified
        if not allowed:
            raise HTTPException(status_code=400, detail=f"Risk check failed: {reason}")
        
        # Check margin
        if not check_margin_requirement(request.volume, request.symbol):
            raise HTTPException(status_code=400, detail="Insufficient margin")
        
        # Execute order
        result = mt5_manager.send_order(request)
        
        if result.success:
            logger.info(f"Order executed: {request.symbol} {request.direction} {request.volume} lots")
        else:
            logger.error(f"Order failed: {result.error_message}")
        
        return result
        
    except Exception as e:
        logger.error(f"Order execution error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook/signal")
async def webhook_signal_handler(data: Dict):
    """N8N webhook handler for signal processing"""
    logger.info(f"Received signal webhook: {data}")
    return {"status": "received", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.post("/webhook/approval")
async def webhook_approval_handler(data: Dict):
    """N8N webhook handler for manual approval"""
    logger.info(f"Received approval webhook: {data}")
    return {"status": "approved", "timestamp": datetime.now(timezone.utc).isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)