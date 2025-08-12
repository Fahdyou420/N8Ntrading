-- Trading database initialization script

-- Signals table
CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    signal_id VARCHAR(36) UNIQUE NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    direction VARCHAR(4) NOT NULL CHECK (direction IN ('BUY', 'SELL')),
    entry_price DECIMAL(10, 5) NOT NULL,
    sl DECIMAL(10, 5),
    tp DECIMAL(10, 5),
    sl_pips DECIMAL(6, 2),
    tp_pips DECIMAL(6, 2),
    suggested_volume_lots DECIMAL(8, 3),
    confidence DECIMAL(3, 2) CHECK (confidence >= 0 AND confidence <= 1),
    reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    signal_id VARCHAR(36) REFERENCES signals(signal_id),
    idempotency_key VARCHAR(36) UNIQUE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    direction VARCHAR(4) NOT NULL CHECK (direction IN ('BUY', 'SELL')),
    volume DECIMAL(8, 3) NOT NULL,
    entry_price DECIMAL(10, 5),
    sl DECIMAL(10, 5),
    tp DECIMAL(10, 5),
    executed_price DECIMAL(10, 5),
    slippage_pips DECIMAL(6, 2),
    order_id BIGINT,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'EXECUTED', 'FAILED', 'CANCELLED')),
    error_code INTEGER,
    error_message TEXT,
    execution_mode VARCHAR(10) DEFAULT 'PAPER' CHECK (execution_mode IN ('PAPER', 'LIVE')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    executed_at TIMESTAMP WITH TIME ZONE
);

-- Risk metrics table
CREATE TABLE IF NOT EXISTS risk_metrics (
    id SERIAL PRIMARY KEY,
    date DATE DEFAULT CURRENT_DATE,
    total_positions INTEGER DEFAULT 0,
    daily_pnl DECIMAL(12, 2) DEFAULT 0,
    max_drawdown DECIMAL(12, 2) DEFAULT 0,
    account_balance DECIMAL(12, 2),
    margin_used DECIMAL(12, 2),
    margin_free DECIMAL(12, 2),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Strategy performance table
CREATE TABLE IF NOT EXISTS strategy_performance (
    id SERIAL PRIMARY KEY,
    strategy VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    total_signals INTEGER DEFAULT 0,
    successful_trades INTEGER DEFAULT 0,
    failed_trades INTEGER DEFAULT 0,
    total_pnl DECIMAL(12, 2) DEFAULT 0,
    win_rate DECIMAL(5, 2) DEFAULT 0,
    avg_trade_duration_hours DECIMAL(8, 2),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Approval logs table
CREATE TABLE IF NOT EXISTS approval_logs (
    id SERIAL PRIMARY KEY,
    signal_id VARCHAR(36) REFERENCES signals(signal_id),
    action VARCHAR(20) NOT NULL CHECK (action IN ('APPROVED', 'REJECTED', 'TIMEOUT')),
    approver VARCHAR(100),
    ai_validation TEXT,
    human_notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_signal_id ON orders(signal_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_execution_mode ON orders(execution_mode);
CREATE INDEX IF NOT EXISTS idx_risk_metrics_date ON risk_metrics(date);

-- Insert initial risk metrics record
INSERT INTO risk_metrics (account_balance, margin_used, margin_free) 
VALUES (10000.00, 0.00, 10000.00) 
ON CONFLICT DO NOTHING;

-- Insert initial strategy performance records
INSERT INTO strategy_performance (strategy, symbol) VALUES
('soros_macro_breakout', 'EURUSD'),
('soros_macro_breakout', 'GBPUSD'),
('soros_macro_breakout', 'USDJPY'),
('jones_trend', 'EURUSD'),
('jones_trend', 'GBPUSD'),
('jones_trend', 'USDJPY'),
('simons_stat_arb', 'EURUSD'),
('simons_stat_arb', 'GBPUSD'),
('simons_stat_arb', 'USDJPY'),
('druckenmiller_macro', 'EURUSD'),
('druckenmiller_macro', 'GBPUSD'),
('druckenmiller_macro', 'USDJPY'),
('burry_carry', 'EURUSD'),
('burry_carry', 'GBPUSD'),
('burry_carry', 'USDJPY')
ON CONFLICT DO NOTHING;
