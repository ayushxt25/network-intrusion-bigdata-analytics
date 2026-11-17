$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "..\demo\ComposeWrapper.ps1")
$fixture = Join-Path $PSScriptRoot "fixtures\fake_native_compose.cmd"
if (-not (Test-Path $fixture)) { throw "Missing fake native executable: $fixture" }
$cmd = if ($env:ComSpec) { $env:ComSpec } else { (Get-Command cmd.exe -ErrorAction Stop).Source }
$successCommand = "call `"$fixture`""
$failureCommand = "call `"$fixture`" fail"

$savedNativePreference = $null
$hasNativePreference = Test-Path Variable:PSNativeCommandUseErrorActionPreference
if ($hasNativePreference) {
    $savedNativePreference = $PSNativeCommandUseErrorActionPreference
    # Exercise the PowerShell 7 configuration that applies ErrorActionPreference
    # to native commands; Windows PowerShell 5.1 simply skips this branch.
    $PSNativeCommandUseErrorActionPreference = $true
}
try {
    $before = $ErrorActionPreference
    $successCode = -1
    $successOutput = @(Invoke-DemoCompose -Executable $cmd -ExitCode ([ref]$successCode) /d /c $successCommand)
    if ($successCode -ne 0) { throw "Expected exit 0, got $successCode" }
    if (($successOutput -join "`n") -notmatch "WARNING: simulated harmless") { throw "stderr warning was not preserved" }
    if (($successOutput -join "`n") -notmatch "simulated stdout") { throw "stdout was not preserved" }
    if ($ErrorActionPreference -ne $before) { throw "ErrorActionPreference was not restored" }

    $failureCode = -1
    $failureOutput = @(Invoke-DemoCompose -Executable $cmd -ExitCode ([ref]$failureCode) /d /c $failureCommand)
    if ($failureCode -ne 7) { throw "Expected exit 7, got $failureCode" }
    if (($failureOutput -join "`n") -notmatch "simulated native failure") { throw "failure stderr was not preserved" }
    "COMPOSE_WRAPPER_TEST_OK"
} finally {
    if ($hasNativePreference) { $PSNativeCommandUseErrorActionPreference = $savedNativePreference }
}
