<#
.SYNOPSIS
  Runs the benchmarks (docs/benchmarks.md describes them) and writes bench-results.md.
#>
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"
Push-Location $Root
try {
    & "$Root\mvnw.cmd" -q -DskipTests test-compile
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    & java -Xmx2g -cp 'target\classes;target\test-classes' dev.raftkv.server.Bench bench-results.md
} finally {
    Pop-Location
}
