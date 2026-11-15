<#
.SYNOPSIS
  Jepsen-lite: 3 real server processes, kill -9 and shard moves while clients run, then a
  linearizability check of everything the clients saw.
.EXAMPLE
  .\scripts\jepsen-lite.ps1            # 60 seconds
  .\scripts\jepsen-lite.ps1 -Seconds 300
#>
param([int]$Seconds = 60)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"
Push-Location $Root
try {
    & "$Root\mvnw.cmd" -q -DskipTests test-compile
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    & java -cp 'target\classes;target\test-classes' dev.raftkv.server.JepsenLite $Seconds
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
