\# Stock Alert System



FastAPI trading dashboard for scanning stocks, ranking weekly technical setups, managing positions, syncing with IBKR TWS Paper Trading, and monitoring risk.



\## Run locally



```bash

py -3.12 -m pip install -r requirements.txt

py -3.12 -m uvicorn main:app --reload --port 8000



## ChatGPT Bridge + Cloudflare Tunnel

The supported architecture is:

```
ChatGPT -> HTTPS -> Cloudflare Tunnel -> local FastAPI -> TWS Paper 127.0.0.1:7497
```

Render is not required for this path. TWS remains local and is never exposed to the public internet.

### Public dashboard

The configured hostname is:

```
https://stocks.skipperil.co.il/
```

Remote dashboard access is protected with HTTP Basic Auth. Local access at
`http://127.0.0.1:8000/` remains available without remote-dashboard authentication.

Required local `.env` values:

```
IBKR_PAPER_TRADING=true
IBKR_ENABLE_REAL_TRADING=false

CHATGPT_BRIDGE_TOKEN=<strong unique token, 32+ characters>
CHATGPT_BRIDGE_ALLOW_SUBMIT=false
CHATGPT_BRIDGE_IBKR_HOST=127.0.0.1
CHATGPT_BRIDGE_IBKR_PORT=7497
CHATGPT_BRIDGE_READ_CLIENT_ID=71
CHATGPT_BRIDGE_TRADE_CLIENT_ID=72
CHATGPT_BRIDGE_MAX_QUANTITY=10
CHATGPT_BRIDGE_MAX_NOTIONAL_USD=5000

DASHBOARD_REMOTE_AUTH_ENABLED=true
DASHBOARD_PUBLIC_HOST=stocks.skipperil.co.il
DASHBOARD_BASIC_USER=<username>
DASHBOARD_BASIC_PASSWORD=<strong password>

CLOUDFLARE_TUNNEL_TOKEN=<local tunnel token>
```

Never commit real tokens or passwords.

### Start locally on Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\run_local_stack.ps1 -InstallCloudflared
```

The launcher generates strong bridge/dashboard secrets if they are missing,
installs Python dependencies, starts FastAPI on loopback port 8000, and starts
the existing Cloudflare Tunnel. It intentionally does not print secrets.

### ChatGPT API flow

All endpoints under `/api/chatgpt/` require a Bearer token.

Read-only:
- `GET /api/chatgpt/health`
- `GET /api/chatgpt/account`
- `GET /api/chatgpt/positions`
- `GET /api/chatgpt/orders`
- `GET /api/chatgpt/executions`
- `GET /api/chatgpt/quote/{symbol}`

Controlled Paper order flow:
- `POST /api/chatgpt/order/prepare`
- `POST /api/chatgpt/order/confirm`
- `POST /api/chatgpt/order/submit`
- `GET /api/chatgpt/order/{proposal_id}`

Submission is disabled by default. Set `CHATGPT_BRIDGE_ALLOW_SUBMIT=true` only
after read-only and prepare/confirm verification succeeds.

The Bridge rejects Live mode, requires the Paper TWS port (7497), uses separate
read/trade client IDs, applies quantity/notional limits, uses expiring one-time
approval tokens, and writes an append-only JSONL audit log.
