<#
.SYNOPSIS
  Starts (or restarts after a kill) one node of the local cluster. It recovers from data\nN.
  Uses the cluster shape saved by run-cluster.ps1 (voters and spares).
.EXAMPLE
  .\scripts\start-node.ps1 -Id 2
#>
param(
    [Parameter(Mandatory = $true)][int]$Id,
    [switch]$Hidden
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"
$settings = Get-Settings
$total = $settings.nodes + $settings.spares
if ($Id -lt 1 -or $Id -gt $total) { throw "Id must be between 1 and $total" }
Start-RaftNode -Id $Id -Settings $settings -Hidden:$Hidden
