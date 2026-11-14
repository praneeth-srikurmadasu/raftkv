<#
.SYNOPSIS
  Stops every node of the local cluster that the scripts started.
#>
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"
if (-not (Test-Path $PidDir)) {
    Write-Host 'no nodes running'
    exit 0
}
Get-ChildItem -Path $PidDir -Filter 'n*.pid' | ForEach-Object {
    $id = [int]($_.BaseName.Substring(1))
    $p = Get-NodeProcess $id
    if ($null -ne $p) {
        Stop-Process -Id $p.Id -Force
        Write-Host "stopped n$id (pid $($p.Id))"
    }
    Remove-Item -Force $_.FullName
}
