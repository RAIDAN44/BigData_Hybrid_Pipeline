$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$VivaRoot = $PSScriptRoot
$ProjectRoot = Split-Path -Parent $VivaRoot
$Runtime = Join-Path $VivaRoot 'runtime'
$Logs = Join-Path $Runtime 'logs'
$Requirements = Join-Path $VivaRoot 'backend\requirements.txt'

New-Item -ItemType Directory -Path $Logs -Force | Out-Null

Write-Host '============================================================' -ForegroundColor DarkCyan
Write-Host ' VIVA_APP - Hybrid Big Data Pipeline' -ForegroundColor Cyan
Write-Host '============================================================' -ForegroundColor DarkCyan
Write-Host "Project Root : $ProjectRoot"
Write-Host "VIVA_APP     : $VivaRoot"

# Prefer the active Conda environment, then the verified project environment,
# then a Python available on PATH.
$Python = $null
if ($env:CONDA_PREFIX) {
    $candidate = Join-Path $env:CONDA_PREFIX 'python.exe'
    if (Test-Path $candidate) { $Python = $candidate }
}

if (-not $Python) {
    $candidate = Join-Path $HOME 'anaconda3\envs\IPPR\python.exe'
    if (Test-Path $candidate) { $Python = $candidate }
}

if (-not $Python) {
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) { $Python = $cmd.Source }
}

if (-not $Python) {
    throw 'Python was not found. Activate the project Python/Conda environment first.'
}

Write-Host "Python       : $Python" -ForegroundColor Gray

# Install VIVA-only web dependencies only when imports are missing.
& $Python -c "import fastapi, uvicorn, pymongo, multipart" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host 'Installing missing VIVA_APP backend requirements...' -ForegroundColor Yellow
    & $Python -m pip install -r $Requirements
    if ($LASTEXITCODE -ne 0) { throw 'Could not install VIVA_APP backend requirements.' }
} else {
    Write-Host 'Backend requirements: READY' -ForegroundColor Green
}

# Pick an unused localhost TCP port.
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start()
$Port = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
$listener.Stop()

$Stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$StdOut = Join-Path $Logs "server_${Stamp}.out.log"
$StdErr = Join-Path $Logs "server_${Stamp}.err.log"
$Url = "http://127.0.0.1:$Port/"

Write-Host "Port         : $Port"
Write-Host 'Starting presentation server...' -ForegroundColor Cyan

$proc = Start-Process `
    -FilePath $Python `
    -ArgumentList @('-m','uvicorn','VIVA_APP.backend.app:app','--host','127.0.0.1','--port',"$Port") `
    -WorkingDirectory $ProjectRoot `
    -RedirectStandardOutput $StdOut `
    -RedirectStandardError $StdErr `
    -PassThru `
    -NoNewWindow

try {
    $Ready = $false
    for ($i = 0; $i -lt 50; $i++) {
        Start-Sleep -Milliseconds 250
        if ($proc.HasExited) { break }
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/health" -TimeoutSec 1
            if ($health.status -eq 'ok') { $Ready = $true; break }
        } catch {}
    }

    if (-not $Ready) {
        Write-Host 'Server failed to become healthy.' -ForegroundColor Red
        if (Test-Path $StdErr) { Get-Content $StdErr -Tail 80 }
        throw 'VIVA_APP startup failed.'
    }

    Write-Host 'HEALTH CHECK  : PASS' -ForegroundColor Green
    Write-Host "Opening       : $Url" -ForegroundColor Green
    Write-Host "Runtime logs  : $Logs" -ForegroundColor DarkGray
    Start-Process $Url

    Write-Host ''
    Write-Host 'SERVER STAYS ALIVE.' -ForegroundColor Cyan
    Write-Host 'Press [ENTER] only when you want to close VIVA_APP.' -ForegroundColor Yellow
    [void](Read-Host)
}
finally {
    if ($proc -and -not $proc.HasExited) {
        Write-Host 'Stopping VIVA_APP server...' -ForegroundColor Yellow
        # Stop uvicorn and any child processes it may have created.
        & taskkill.exe /PID $proc.Id /T /F 2>$null | Out-Null
    }
    Write-Host 'VIVA_APP stopped.' -ForegroundColor Green
}
