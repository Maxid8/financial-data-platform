<#
.SYNOPSIS
    Daily collection run for the Windows Task Scheduler.

.DESCRIPTION
    For every ticker:
      1. Checks whether today's raw object already exists in S3 (verifier).
         If it does, the ticker is skipped and no API call is made.
      2. Otherwise runs the collector (src/API_call.py), then the verifier
         (src/verify_s3.py) again on the new object.

    The script is safe to run several times a day: later runs only retry the
    tickers that are still missing. A failing ticker does not stop the others.
    The public IP address of the run is logged, to help diagnose the
    intermittent Alpha Vantage rejections. All output is appended to
    logs/collection_<date>.log.

    Exit code: 0 if every ticker is collected (now or earlier today), 1 otherwise.
    The Task Scheduler shows it in the "Last Run Result" column.

.EXAMPLE
    powershell.exe -ExecutionPolicy Bypass -File scripts\run_daily_collection.ps1
#>

$Tickers = @("IBM", "MSFT", "AAPL", "JPM", "XOM")

# Pause after each API call, so the calls are not sent in a burst
# (Alpha Vantage free tier: 5 requests per minute).
$PauseSeconds = 15

# The script lives in <project>\scripts, so the project root is one level up.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$LogDir = Join-Path $ProjectRoot "logs"
$LogFile = Join-Path $LogDir ("collection_{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))

# Same network route and TLS setup as the collector (urllib + truststore).
# Single quotes only: Windows PowerShell mangles double quotes in native arguments.
$PublicIpCode = "import ssl, truststore, urllib.request; " +
    "ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT); " +
    "print(urllib.request.urlopen('https://checkip.amazonaws.com', context=ctx, timeout=10).read().decode().strip())"

function Write-Log([string]$Message) {
    $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

function Invoke-PythonScript([string]$Script, [string]$Ticker) {
    # Python logs to stderr. "2>&1" merges it into the output, so every
    # line reaches the log file. "$_" turns each line into plain text.
    & $Python $Script --ticker $Ticker 2>&1 | ForEach-Object {
        Add-Content -Path $LogFile -Value "$_" -Encoding UTF8
    }
    return $LASTEXITCODE
}

function Test-CollectedToday([string]$Ticker) {
    # Runs the verifier without logging its output: a missing object is the
    # normal case before the first run of the day, not an error.
    & $Python "src\verify_s3.py" --ticker $Ticker 2>&1 | Out-Null
    return ($LASTEXITCODE -eq 0)
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $ProjectRoot

if (-not (Test-Path $Python)) {
    Write-Log "ERROR: Python not found in the virtual environment: $Python"
    exit 1
}

Write-Log "Daily collection started for: $($Tickers -join ', ')"

$PublicIp = & $Python -c $PublicIpCode 2>$null
if ($LASTEXITCODE -eq 0 -and $PublicIp) {
    Write-Log "Public IP of this run: $PublicIp"
}
else {
    Write-Log "Public IP of this run: unknown (lookup failed)"
}

$Failed = @()

foreach ($Ticker in $Tickers) {
    if (Test-CollectedToday $Ticker) {
        Write-Log "SKIPPED: $Ticker (already collected today)"
        continue
    }

    $CollectCode = Invoke-PythonScript "src\API_call.py" $Ticker
    $VerifyCode = Invoke-PythonScript "src\verify_s3.py" $Ticker

    if ($CollectCode -eq 0 -and $VerifyCode -eq 0) {
        Write-Log "OK: $Ticker"
    }
    else {
        Write-Log "FAILED: $Ticker (collector exit code $CollectCode, verifier exit code $VerifyCode)"
        $Failed += $Ticker
    }

    Start-Sleep -Seconds $PauseSeconds
}

if ($Failed.Count -gt 0) {
    Write-Log "Finished with failures: $($Failed -join ', ')"
    exit 1
}

Write-Log "Finished: all tickers collected today."
exit 0
