<#
.SYNOPSIS
    Daily collection run for the Windows Task Scheduler.

.DESCRIPTION
    For every ticker, runs the collector (src/API_call.py) and then the S3
    verifier (src/verify_s3.py) with the project's virtual environment.
    A failing ticker does not stop the others. All output is appended to
    logs/collection_<date>.log.

    Exit code: 0 if every ticker was collected and verified, 1 otherwise.
    The Task Scheduler shows it in the "Last Run Result" column.

.EXAMPLE
    powershell.exe -ExecutionPolicy Bypass -File scripts\run_daily_collection.ps1
#>

$Tickers = @("IBM", "MSFT", "AAPL", "JPM", "XOM")

# Pause between tickers, so the API calls are not sent in a burst.
$PauseSeconds = 15

# The script lives in <project>\scripts, so the project root is one level up.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$LogDir = Join-Path $ProjectRoot "logs"
$LogFile = Join-Path $LogDir ("collection_{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))

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

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $ProjectRoot

if (-not (Test-Path $Python)) {
    Write-Log "ERROR: Python not found in the virtual environment: $Python"
    exit 1
}

Write-Log "Daily collection started for: $($Tickers -join ', ')"
$Failed = @()

for ($i = 0; $i -lt $Tickers.Count; $i++) {
    $Ticker = $Tickers[$i]

    $CollectCode = Invoke-PythonScript "src\API_call.py" $Ticker
    $VerifyCode = Invoke-PythonScript "src\verify_s3.py" $Ticker

    if ($CollectCode -eq 0 -and $VerifyCode -eq 0) {
        Write-Log "OK: $Ticker"
    }
    else {
        Write-Log "FAILED: $Ticker (collector exit code $CollectCode, verifier exit code $VerifyCode)"
        $Failed += $Ticker
    }

    if ($i -lt $Tickers.Count - 1) {
        Start-Sleep -Seconds $PauseSeconds
    }
}

if ($Failed.Count -gt 0) {
    Write-Log "Finished with failures: $($Failed -join ', ')"
    exit 1
}

Write-Log "Finished: all tickers collected and verified."
exit 0
