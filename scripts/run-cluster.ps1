<#
.SYNOPSIS
  Starts a local RaftKV cluster: one java process per node.
.DESCRIPTION
  Each node gets its own console window (or runs hidden with -Hidden, logging to logs\nN.log),
  listens on 127.0.0.1:<BasePort + N - 1> and keeps its data in data\nN.
.EXAMPLE
  .\scripts\run-cluster.ps1              # 3 nodes on ports 7001-7003
  .\scripts\run-cluster.ps1 -Clean       # delete all data first (a brand-new cluster)
  .\scripts\run-cluster.ps1 -Nodes 5     # 5 nodes on ports 7001-7005
#>
param(
    [int]$Nodes = 3,
    [int]$BasePort = 7001,
    [switch]$Clean,
    [switch]$Hidden,
    [switch]$NoBuild
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"

if ($Clean) {
    for ($i = 1; $i -le $Nodes; $i++) {
        if ($null -ne (Get-NodeProcess $i)) { throw "n$i is still running; run .\scripts\stop-cluster.ps1 first" }
    }
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue (Join-Path $Root 'data'), $LogDir
    Write-Host 'deleted data\ and logs\'
}
if (-not $NoBuild) { Invoke-Build }
for ($i = 1; $i -le $Nodes; $i++) {
    Start-RaftNode -Id $i -Nodes $Nodes -BasePort $BasePort -Hidden:$Hidden
}
Write-Host ''
Write-Host 'Talk to it with:   .\scripts\cli.ps1'
Write-Host 'Kill a node with:  .\scripts\kill-node.ps1 -Id 2'
Write-Host 'Stop everything:   .\scripts\stop-cluster.ps1'
