# Shared helpers for the RaftKV PowerShell scripts. Dot-source it: . "$PSScriptRoot\common.ps1"
# Works in Windows PowerShell 5.1 and PowerShell 7.

$Root = Split-Path -Parent $PSScriptRoot
$PidDir = Join-Path $Root 'data\pids'
$LogDir = Join-Path $Root 'logs'

function Get-Peers([int]$Nodes, [int]$BasePort) {
    $parts = @()
    for ($i = 1; $i -le $Nodes; $i++) {
        $parts += "$i=127.0.0.1:$($BasePort + $i - 1)"
    }
    return ($parts -join ',')
}

function Invoke-Build {
    Write-Host 'Compiling (mvnw -q -DskipTests compile)...'
    Push-Location $Root
    try {
        & "$Root\mvnw.cmd" -q -DskipTests compile
        if ($LASTEXITCODE -ne 0) { throw "build failed (exit code $LASTEXITCODE)" }
    } finally {
        Pop-Location
    }
}

function Get-NodeProcess([int]$Id) {
    $pidFile = Join-Path $PidDir "n$Id.pid"
    if (-not (Test-Path $pidFile)) { return $null }
    $nodePid = [int](Get-Content $pidFile -Raw)
    $p = Get-Process -Id $nodePid -ErrorAction SilentlyContinue
    if ($null -ne $p -and $p.ProcessName -like 'java*') { return $p }
    return $null
}

function Start-RaftNode([int]$Id, [int]$Nodes, [int]$BasePort, [bool]$Hidden) {
    if ($null -ne (Get-NodeProcess $Id)) {
        Write-Host "n$Id is already running"
        return
    }
    New-Item -ItemType Directory -Force -Path $PidDir, $LogDir | Out-Null
    $peers = Get-Peers $Nodes $BasePort
    $javaArgs = @('-cp', 'target\classes', 'dev.raftkv.Main', '--id', "$Id", '--peers', $peers, '--data', "data\n$Id")
    if ($Hidden) {
        $p = Start-Process -FilePath 'java' -ArgumentList $javaArgs -WorkingDirectory $Root -PassThru `
            -WindowStyle Hidden -RedirectStandardOutput "$LogDir\n$Id.log" -RedirectStandardError "$LogDir\n$Id.err.log"
    } else {
        $p = Start-Process -FilePath 'java' -ArgumentList $javaArgs -WorkingDirectory $Root -PassThru
    }
    Set-Content -Path (Join-Path $PidDir "n$Id.pid") -Value $p.Id -NoNewline
    Write-Host "started n$Id (pid $($p.Id)) on port $($BasePort + $Id - 1)"
}
