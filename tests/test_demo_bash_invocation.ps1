$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot "..\demo\ComposeWrapper.ps1")
$scriptPath = Join-Path $PSScriptRoot "..\demo\run_hadoop_category.sh"
$bytes = [IO.File]::ReadAllBytes($scriptPath)
for ($i = 0; $i -lt ($bytes.Length - 1); $i++) {
    if ($bytes[$i] -eq 13 -and $bytes[$i + 1] -eq 10) { throw "Bash script contains CRLF: $scriptPath" }
}

if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    $compose = "docker-compose"; $prefix = @()
} else {
    $compose = "docker"; $prefix = @("compose")
}
$input = '/data/unsw/raw/space & dollar$(literal)'
$output = '/data/unsw/demo/quote-"double''single;-$()'
$exitCode = -1
$result = @(Invoke-DemoCompose -Executable $compose -Prefix $prefix -ExitCode ([ref]$exitCode) -Arguments @('--profile','hadoop','run','--rm','--no-deps','--entrypoint','bash','hadoop-jobs','/workspace/demo/run_hadoop_category.sh','--check-args',$input,$output))
if ($exitCode -ne 0) { throw "Bash argument invocation failed with exit ${exitCode}: $($result -join [Environment]::NewLine)" }
$text = $result -join "`n"
if ($text -notmatch [regex]::Escape("ARG[0]=<$input>") -or $text -notmatch [regex]::Escape("ARG[1]=<$output>")) {
    throw "Bash arguments were changed: $text"
}
"DEMO_BASH_INVOCATION_TEST_OK"
