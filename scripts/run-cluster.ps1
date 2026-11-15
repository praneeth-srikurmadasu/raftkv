<#
.SYNOPSIS
  Starts a local RaftKV cluster: one java process per node.
.DESCRIPTION
  Each node gets its own console window (or runs hidden with -Hidden, logging to logs\nN.log),
  listens on 127.0.0.1:<BasePort + N - 1>, keeps its data in data\nN, and serves a live dashboard
  on http://127.0.0.1:<BasePort + 1000 + N - 1>/. With -Spares, extra servers are configured but not
  started: start one with start-node.ps1 and add it to a group with 'admin add-server' in the CLI.
.EXAMPLE
  .\scripts\run-cluster.ps1              # 3 nodes on ports 7001-7003
  .\scripts\run-cluster.ps1 -Clean       # delete all data first (a brand-new cluster)
  .\scripts\run-cluster.ps1 -Spares 2    # 3 voters, plus n4 and n5 ready to join later
#>
param(
    [int]$Nodes = 3,
    [int]$Spares = 0,
    [int]$BasePort = 7001,
    [switch]$Clean,
    [switch]$Hidden,
    [switch]$NoBuild
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"

if ($Clean) {
    for ($i = 1; $i -le ($Nodes + $Spares + 5); $i++) {
        if ($null -ne (Get-NodeProcess $i)) { throw "n$i is still running; run .\scripts\stop-cluster.ps1 first" }
    }
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue (Join-Path $Root 'data'), $LogDir
    Write-Host 'deleted data\ and logs\'
}
if (-not $NoBuild) { Invoke-Build }
Save-Settings $Nodes $Spares $BasePort
$settings = Get-Settings
for ($i = 1; $i -le $Nodes; $i++) {
    Start-RaftNode -Id $i -Settings $settings -Hidden:$Hidden
}
Write-Host ''
Write-Host "Dashboard:         http://127.0.0.1:$($BasePort + 1000)/"
Write-Host 'Talk to it with:   .\scripts\cli.ps1'
Write-Host 'Kill a node with:  .\scripts\kill-node.ps1 -Id 2'
if ($Spares -gt 0) {
    Write-Host "Start a spare:     .\scripts\start-node.ps1 -Id $($Nodes + 1)   (then in the CLI: admin add-server 1 n$($Nodes + 1))"
}
Write-Host 'Stop everything:   .\scripts\stop-cluster.ps1'
