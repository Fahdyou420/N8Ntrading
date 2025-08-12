# Forex Trading Automation Stack

A production-ready automated forex trading system that generates multi-strategy signals and executes them via MetaTrader 5, orchestrated through N8N workflows on Render cloud.

## 🚀 Features

- **5 Professional Trading Strategies**: Soros macro breakout, Paul Tudor Jones trend-following, Renaissance statistical arbitrage, Druckenmiller macro, and Michael Burry carry trades
- **Risk Management**: Position sizing, margin checks, daily loss limits, and exposure controls
- **Human-in-the-Loop**: Optional ChatGPT validation with manual approval workflow
- **Paper Trading**: Safe testing environment before live execution
- **N8N Orchestration**: Visual workflow management with Slack notifications
- **Production Ready**: Docker deployment, PostgreSQL logging, comprehensive testing

## 📋 Quick Start for N8N on Render

### 1. Deploy to Render

1. **Fork this repository** to your GitHub account

2. **Create a new Web Service** on Render:
   - Connect your GitHub repository
   - Runtime: Docker
   - Build command: `docker build -t trading-stack .`
   - Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

3. **Add Environment Variables** in Render dashboard:
   ```bash
   PAPER_MODE=true
   MANUAL_APPROVAL=true
   MT5_REST_API_KEY=your_secret_key_here
   RISK_PCT_DEFAULT=2.0
   DATABASE_URL=your_postgres_url_from_render
   SLACK_WEBHOOK_URL=your_slack_webhook
   ```

4. **Create PostgreSQL Database** on Render and add the URL to `DATABASE_URL`

### 2. Set Up N8N Workflow

1. **Import the Workflow**:
   - Copy the contents of `n8n_trading_workflow.json`
   - In your N8N instance, go to Workflows → Import from JSON
   - Paste the JSON content

2. **Configure Environment Variables in N8N**:
   ```bash
   TRADING_SERVICE_URL=https://your-render-app.onrender.com
   MT5_REST_API_KEY=your_secret_key_here
   OPENAI_API_KEY=your_openai_api_key
   MANUAL_APPROVAL=true
   ```

3. **Set Up Slack Integration**:
   - Create a Slack App and get webhook URL
   - Add Slack credentials in N8N
   - Update webhook URLs in environment

### 3. MetaTrader 5 Setup

#### Option A: Local MT5 (Recommended for Testing)
1. Install MetaTrader 5 on your local machine
2. Open a demo account with any broker
3. Enable "Allow automated trading" in Tools → Options → Expert Advisors
4. Install the MT5 Python package: `pip install MetaTrader5`

#### Option B: MetaApi Cloud (Production)
1. Sign up for MetaApi.cloud account
2. Connect your MT5 broker account
3. Get API token and update configuration
4. Modify the MT5Manager class to use MetaApi instead of local MT5

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PAPER_MODE` | Enable paper trading | `true` | Yes |
| `MANUAL_APPROVAL` | Require human approval | `true` | Yes |
| `MT5_REST_API_KEY` | API key for authentication | - | Yes |
| `RISK_PCT_DEFAULT` | Default risk per trade (%) | `2.0` | No |
| `MAX_DAILY_RISK_PCT` | Maximum daily loss (%) | `10.0` | No |
| `MAX_OPEN_POSITIONS` | Maximum concurrent trades | `5` | No |
| `DATABASE_URL` | PostgreSQL connection string | - | Yes |
| `SLACK_WEBHOOK_URL` | Slack notifications | - | No |

### Strategy Configuration

Enable/disable individual strategies:
```bash
ENABLE_SOROS=true
ENABLE_JONES=true
ENABLE_SIMONS=true
ENABLE_DRUCKENMILLER=true
ENABLE_BURRY=true
```

## 📊 Trading Strategies

### 1. Soros Macro Breakout (`soros_macro_breakout`)
- **Logic**: Economic surprise detection + momentum breakout
- **Timeframe**: M1/M5 for entry, economic calendar for triggers
- **Risk**: ATR-based stops, 2.5R target
- **Best For**: Major news events, central bank decisions

### 2. Paul Tudor Jones Trend (`jones_trend`)
- **Logic**: EMA50/200 crossover on H4 with D1 confirmation
- **Filter**: RSI exclusion (40-60 zone)
- **Risk**: ATR*1.5 stops, 2R target
- **Best For**: Strong trending markets

### 3. Renaissance Stat Arb (`simons_stat_arb`)
- **Logic**: Z-score mean reversion on currency pairs
- **Entry**: |Z-score| ≥ 2.0
- **Risk**: Tight stops (10 pips), quick scalping
- **Best For**: Range-bound, high-liquidity periods

### 4. Druckenmiller Macro (`druckenmiller_macro`)
- **Logic**: DXY trend + equity sentiment + yield analysis
- **Risk**: Wide stops (50 pips), large targets (150 pips)
- **Best For**: Major macro regime changes

### 5. Michael Burry Carry (`burry_carry`)
- **Logic**: Interest rate differential + valuation analysis
- **Risk**: Long-term holds, 80 pip stops
- **Best For**: Stable economic periods with clear rate differentials

## 🔄 N8N Workflow Description

The workflow includes these key nodes:

1. **Schedule Trigger**: Runs every 5 minutes
2. **Generate Signals**: Calls the FastAPI batch endpoint
3. **Filter High Confidence**: Filters signals with confidence ≥ 60%
4. **Manual Approval Check**: Routes based on MANUAL_APPROVAL setting
5. **ChatGPT Validation**: AI analysis of trading signals
6. **Slack Notification**: Human approval request with links
7. **Wait for Approval**: Webhook listener for human input
8. **Execute Order**: Sends orders to MT5 via FastAPI
9. **Trade Notifications**: Success/failure alerts

### Manual Approval Process

When `MANUAL_APPROVAL=true`:
1. Signal generated and validated by ChatGPT
2. Slack message sent with AI analysis
3. Human clicks APPROVE or REJECT link
4. Webhook processes the decision
5. Order executed only if approved

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Install test dependencies
pip install pytest httpx

# Run all tests
pytest test_trading_stack.py -v

# Run specific test categories
pytest test_trading_stack.py::TestRiskManagement -v
pytest test_trading_stack.py::TestAPIEndpoints -v
pytest test_trading_stack.py::TestSignalGeneration -v
```

### Test Coverage

- ✅ Risk management calculations
- ✅ Signal generation for all strategies
- ✅ API endpoint functionality
- ✅ Error handling and validation
- ✅ Paper mode simulation
- ✅ Integration workflows

## 📈 Monitoring & Observability

### Health Checks
- **Endpoint**: `GET /health`
- **Monitors**: MT5 connection, database status, configuration
- **Render**: Automatic health check configuration included

### Logging
- **Application**: Structured JSON logs to stdout
- **Database**: All trades, signals, and approvals logged
- **Slack**: Real-time trade notifications

### Performance Metrics
- Win rate by strategy
- Average trade duration
- Daily/weekly P&L
- Risk-adjusted returns

## 🔒 Security

### API Security
- API key authentication on all endpoints
- Rate limiting (configure in Render)
- HTTPS enforcement

### Risk Controls
- Position size limits
- Daily loss limits
- Maximum concurrent positions
- Margin requirement checks

### Data Protection
- Sensitive config in environment variables
- No hardcoded credentials
- PostgreSQL connection encryption

## 🚨 Production Deployment

### Before Going Live

1. **Test in Paper Mode** for at least 2 weeks
2. **Validate all strategies** with historical data
3. **Test manual approval** workflow end-to-end
4. **Configure position sizing** for your account size
5. **Set up monitoring** and alerting
6. **Have a kill switch** plan ready

### Go-Live Checklist

- [ ] Paper mode tested extensively
- [ ] All environment variables configured
- [ ] MT5 connection working
- [ ] Database initialized
- [ ] Slack notifications working
- [ ] Manual approval tested
- [ ] Risk limits appropriate
- [ ] Monitoring dashboard ready
- [ ] Emergency stop procedure documented

### Risk Warning

**This software is for educational and testing purposes. Live trading involves substantial risk of loss. Never risk more than you can afford to lose. Always test thoroughly in paper mode before live deployment.**

## 📞 Support & Troubleshooting

### Common Issues

1. **MT5 Connection Failed**
   - Check MT5 is running and logged in
   - Verify "Allow automated trading" is enabled
   - Ensure Python has proper permissions

2. **N8N Webhook Not Working**
   - Check webhook URLs in environment variables
   - Verify N8N instance is accessible
   - Test webhook endpoints manually

3. **Database Connection Error**
   - Verify PostgreSQL URL is correct
   - Check database is accessible from Render
   - Run `init.sql` to create tables

4. **Signals Not Generating**
   - Check API key configuration
   - Verify strategy toggle environment variables
   - Check application logs for errors

### Getting Help

- **Documentation**: Check this README and code comments
- **Logs**: Monitor application logs in Render dashboard
- **Testing**: Run test suite to validate functionality
- **Health Check**: Use `/health` endpoint to verify status

## 📜 License

This project is provided as-is for educational purposes. See LICENSE file for details.

---

**⚠️ Disclaimer**: This software is for educational and research purposes only. Trading foreign exchange carries a high level of risk and may not be suitable for all investors. Past performance is not indicative of future results. Always consult with a qualified financial advisor before making trading decisions.