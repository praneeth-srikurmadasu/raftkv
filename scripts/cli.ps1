<#
.SYNOPSIS
  Opens the RaftKV command-line client against the local cluster.
.EXAMPLE
  .\scripts\cli.ps1                    # interactive: put/get/delete/cas/status/admin/help/quit
  .\scripts\cli.ps1 put color blue     # one command, then exit
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Command
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"
$settings = Get-Settings
$peers = Get-Peers ($settings.nodes + $settings.spares) $settings.baseport
Push-Location $Root
try {
    & java -cp 'target\classes' dev.raftkv.client.cli.RaftKVCli --servers $peers @Command
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
