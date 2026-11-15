# Shared helpers for the RaftKV PowerShell scripts. Dot-source it: . "$PSScriptRoot\common.ps1"
# Works in Windows PowerShell 5.1 and PowerShell 7.

$Root = Split-Path -Parent $PSScriptRoot
$PidDir = Join-Path $Root 'data\pids'
$LogDir = Join-Path $Root 'logs'
$SettingsFile = Join-Path $Root 'data\cluster.txt'

# The cluster's shape, saved by run-cluster.ps1 so the other scripts agree with it:
# nodes = initial voters (n1..nN), spares = extra servers that can join later, baseport = port of n1.
function Save-Settings([int]$Nodes, [int]$Spares, [int]$BasePort) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $SettingsFile) | Out-Null
    Set-Content -Path $SettingsFile -Value "nodes=$Nodes`nspares=$Spares`nbaseport=$BasePort"
}

function Get-Settings {
    $s = @{ nodes = 3; spares = 0; baseport = 7001 }
    if (Test-Path $SettingsFile) {
        foreach ($line in Get-Content $SettingsFile) {
            $kv = $line.Split('=')
            if ($kv.Length -eq 2) { $s[$kv[0].Trim()] = [int]$kv[1].Trim() }
        }
    }
    return $s
}

function Get-Voters([int]$Nodes) {
    return ((1..$Nodes) -join ',')
}

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

function Start-RaftNode([int]$Id, [hashtable]$Settings, [bool]$Hidden) {
    if ($null -ne (Get-NodeProcess $Id)) {
        Write-Host "n$Id is already running"
        return
    }
    New-Item -ItemType Directory -Force -Path $PidDir, $LogDir | Out-Null
    $total = $Settings.nodes + $Settings.spares
    $peers = Get-Peers $total $Settings.baseport
    $voters = Get-Voters $Settings.nodes
    $javaArgs = @('-cp', 'target\classes', 'dev.raftkv.Main', '--id', "$Id", '--peers', $peers, '--voters', $voters, '--data', "data\n$Id")
    if ($Hidden) {
        $p = Start-Process -FilePath 'java' -ArgumentList $javaArgs -WorkingDirectory $Root -PassThru `
            -WindowStyle Hidden -RedirectStandardOutput "$LogDir\n$Id.log" -RedirectStandardError "$LogDir\n$Id.err.log"
    } else {
        $p = Start-Process -FilePath 'java' -ArgumentList $javaArgs -WorkingDirectory $Root -PassThru
    }
    Set-Content -Path (Join-Path $PidDir "n$Id.pid") -Value $p.Id -NoNewline
    $role = if ($Id -le $Settings.nodes) { 'voter' } else { 'spare' }
    Write-Host "started n$Id (pid $($p.Id), $role) on port $($Settings.baseport + $Id - 1)"
}
