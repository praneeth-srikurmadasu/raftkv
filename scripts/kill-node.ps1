<#
.SYNOPSIS
  Kills one node instantly, with no clean shutdown (like a power cut for that process).
.DESCRIPTION
  Nothing acknowledged is lost: every acknowledged write was fsynced on a majority first.
  Bring the node back with .\scripts\start-node.ps1 -Id N
.EXAMPLE
  .\scripts\kill-node.ps1 -Id 1
#>
param([Parameter(Mandatory = $true)][int]$Id)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"
$p = Get-NodeProcess $Id
if ($null -eq $p) {
    Write-Host "n$Id is not running"
    exit 0
}
Stop-Process -Id $p.Id -Force
Remove-Item -Force (Join-Path $PidDir "n$Id.pid")
Write-Host "killed n$Id (pid $($p.Id))"
