param(
    [switch]$InstallCloudflared
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Ensure-EnvValue {
    param([string]$Name, [string]$Value)
    $envFile = Join-Path $PSScriptRoot ".env"
    if (-not (Test-Path $envFile)) { New-Item -ItemType File -Path $envFile | Out-Null }
    $lines = @(Get-Content $envFile -ErrorAction SilentlyContinue)
    $found = $false
    $new = foreach ($line in $lines) {
        if ($line -match "^$([regex]::Escape($Name))=") {
            $found = $true
            "$Name=$Value"
        } else { $line }
    }
    if (-not $found) { $new += "$Name=$Value" }
    Set-Content -Path $envFile -Value $new -Encoding UTF8
}

function Get-EnvFileValue {
    param([string]$Name)
    $envFile = Join-Path $PSScriptRoot ".env"
    if (-not (Test-Path $envFile)) { return "" }
    foreach ($line in Get-Content $envFile) {
        if ($line -match "^$([regex]::Escape($Name))=(.*)$") { return $Matches[1].Trim() }
    }
    return ""
}

if ($InstallCloudflared -and -not (Get-Command cloudflared -ErrorAction SilentlyContinue)) {
    if (Get-Command winget -ErrorAction SilentlyContinue) {
        winget install --id Cloudflare.cloudflared --accept-package-agreements --accept-source-agreements
    } else {
        throw "cloudflared is missing and winget is unavailable. Install Cloudflare Tunnel client first."
    }
}

if (-not (Get-Command cloudflared -ErrorAction SilentlyContinue)) {
    throw "cloudflared is not installed. Re-run with -InstallCloudflared or install it manually."
}

$bridgeToken = Get-EnvFileValue "CHATGPT_BRIDGE_TOKEN"
if ([string]::IsNullOrWhiteSpace($bridgeToken)) {
    $bytes = New-Object byte[] 48
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    $bridgeToken = [Convert]::ToBase64String($bytes)
    Ensure-EnvValue "CHATGPT_BRIDGE_TOKEN" $bridgeToken
}

$dashUser = Get-EnvFileValue "DASHBOARD_BASIC_USER"
if ([string]::IsNullOrWhiteSpace($dashUser)) {
    Ensure-EnvValue "DASHBOARD_BASIC_USER" "admin"
}

$dashPass = Get-EnvFileValue "DASHBOARD_BASIC_PASSWORD"
if ([string]::IsNullOrWhiteSpace($dashPass)) {
    $bytes = New-Object byte[] 36
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    $dashPass = [Convert]::ToBase64String($bytes)
    Ensure-EnvValue "DASHBOARD_BASIC_PASSWORD" $dashPass
}

Ensure-EnvValue "DASHBOARD_REMOTE_AUTH_ENABLED" "true"
Ensure-EnvValue "DASHBOARD_PUBLIC_HOST" "stocks.skipperil.co.il"
Ensure-EnvValue "CHATGPT_BRIDGE_ALLOW_SUBMIT" "false"
Ensure-EnvValue "CHATGPT_BRIDGE_IBKR_HOST" "127.0.0.1"
Ensure-EnvValue "CHATGPT_BRIDGE_IBKR_PORT" "7497"
Ensure-EnvValue "IBKR_PAPER_TRADING" "true"
Ensure-EnvValue "IBKR_ENABLE_REAL_TRADING" "false"

$tunnelToken = Get-EnvFileValue "CLOUDFLARE_TUNNEL_TOKEN"
if ([string]::IsNullOrWhiteSpace($tunnelToken)) {
    Write-Host ""
    Write-Host "CLOUDFLARE_TUNNEL_TOKEN is not set in .env." -ForegroundColor Yellow
    Write-Host "Add the token for the existing Cloudflare tunnel 'stock-alert-system' and re-run." -ForegroundColor Yellow
    exit 2
}

$python = $null
if (Get-Command py -ErrorAction SilentlyContinue) { $python = "py" }
elseif (Get-Command python -ErrorAction SilentlyContinue) { $python = "python" }
else { throw "Python is not installed or not in PATH." }

if ($python -eq "py") {
    & py -3.12 -m pip install -r requirements.txt
} else {
    & python -m pip install -r requirements.txt
}

$apiArgs = if ($python -eq "py") { "-3.12 -m uvicorn main:app --host 127.0.0.1 --port 8000" } else { "-m uvicorn main:app --host 127.0.0.1 --port 8000" }
$api = Start-Process -FilePath $python -ArgumentList $apiArgs -WorkingDirectory $PSScriptRoot -PassThru

Start-Sleep -Seconds 3
try {
    $local = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:8000/" -TimeoutSec 10
    Write-Host "Local dashboard: HTTP $($local.StatusCode)" -ForegroundColor Green
} catch {
    Stop-Process -Id $api.Id -Force -ErrorAction SilentlyContinue
    throw "FastAPI did not start successfully: $($_.Exception.Message)"
}

$tunnel = Start-Process -FilePath "cloudflared" -ArgumentList @("tunnel","--no-autoupdate","run","--token",$tunnelToken) -WorkingDirectory $PSScriptRoot -PassThru

Write-Host ""
Write-Host "Stock Alert System started." -ForegroundColor Green
Write-Host "Local dashboard:  http://127.0.0.1:8000/"
Write-Host "Remote dashboard: https://stocks.skipperil.co.il/"
Write-Host "FastAPI PID:       $($api.Id)"
Write-Host "cloudflared PID:   $($tunnel.Id)"
Write-Host ""
Write-Host "Remote username:   $(Get-EnvFileValue 'DASHBOARD_BASIC_USER')"
Write-Host "Remote password is stored only in .env and is not printed."
Write-Host "ChatGPT bridge token is stored only in .env and is not printed."
Write-Host "Order submission remains disabled until CHATGPT_BRIDGE_ALLOW_SUBMIT=true."
