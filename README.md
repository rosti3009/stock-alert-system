# Stock Alert System

FastAPI trading dashboard for scanning stocks, ranking weekly technical setups, managing positions, syncing with IBKR TWS Paper Trading, and monitoring risk.

## Architecture

```text
ChatGPT -> HTTPS -> Cloudflare Tunnel -> local FastAPI -> TWS Paper 127.0.0.1:7497
```

Render is not required for this path. TWS remains local and is never exposed directly to the public internet.

## Run locally

```bash
py -3.12 -m pip install -r requirements.txt
py -3.12 -m uvicorn main:app --host 127.0.0.1 --port 8000
```

Local dashboard:

```text
http://127.0.0.1:8000/
```

## Public dashboard through Cloudflare

No SkipperIL or other unrelated domain is required.

By default, `run_local_stack.ps1` uses a free Cloudflare Quick Tunnel when no fixed hostname is configured. It prints a temporary URL in this form:

```text
https://<random>.trycloudflare.com/
```

The temporary URL changes whenever the Quick Tunnel is recreated.

If a dedicated hostname is configured later, set both `DASHBOARD_PUBLIC_HOST` and `CLOUDFLARE_TUNNEL_TOKEN` in the local `.env`. The launcher will then use the named tunnel instead of a Quick Tunnel.

All non-loopback dashboard access is protected with HTTP Basic Auth. Local access on `127.0.0.1` remains available without remote-dashboard authentication.

## Required local environment

Create a local `.env` and keep all real secrets out of Git:

```dotenv
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
DASHBOARD_PUBLIC_HOST=
DASHBOARD_BASIC_USER=<username>
DASHBOARD_BASIC_PASSWORD=<strong password>

# Optional. Leave empty to use a free Quick Tunnel.
CLOUDFLARE_TUNNEL_TOKEN=

# Local broker snapshot target. Render is not required.
BROKER_API_URL=http://127.0.0.1:8000
BROKER_PUSH_TOKEN=<strong unique token, 32+ characters>
```

Never commit real tokens or passwords.

## Start the full local stack on Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\run_local_stack.ps1 -InstallCloudflared
```

The launcher:

- generates strong bridge and dashboard secrets if they are missing;
- installs Python dependencies;
- starts FastAPI on loopback port `8000`;
- uses a free Quick Tunnel when no fixed hostname is configured;
- uses a named Cloudflare Tunnel when both a public hostname and tunnel token are configured;
- does not print stored secrets;
- keeps ChatGPT order submission disabled by default.

## ChatGPT Bridge

All endpoints under `/api/chatgpt/` require a Bearer token.

### Read-only

- `GET /api/chatgpt/health`
- `GET /api/chatgpt/account`
- `GET /api/chatgpt/positions`
- `GET /api/chatgpt/orders`
- `GET /api/chatgpt/executions`
- `GET /api/chatgpt/quote/{symbol}`

### Controlled Paper order flow

- `POST /api/chatgpt/order/prepare`
- `POST /api/chatgpt/order/confirm`
- `POST /api/chatgpt/order/submit`
- `GET /api/chatgpt/order/{proposal_id}`

Submission is disabled by default. Set `CHATGPT_BRIDGE_ALLOW_SUBMIT=true` only after read-only and prepare/confirm verification succeeds.

The Bridge rejects Live mode, requires the Paper TWS port (`7497`), uses separate read/trade client IDs, applies quantity and notional limits, uses expiring one-time approval tokens, and writes an append-only JSONL audit log.
