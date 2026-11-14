<#
.SYNOPSIS
  Starts (or restarts after a kill) one node of the local cluster. It recovers from data\nN.
.EXAMPLE
  .\scripts\start-node.ps1 -Id 2
#>
param(
    [Parameter(Mandatory = $true)][int]$Id,
    [int]$Nodes = 3,
    [int]$BasePort = 7001,
    [switch]$Hidden
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"
if ($Id -lt 1 -or $Id -gt $Nodes) { throw "Id must be between 1 and $Nodes" }
Start-RaftNode -Id $Id -Nodes $Nodes -BasePort $BasePort -Hidden:$Hidden
