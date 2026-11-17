[CmdletBinding()]
param(
    [ValidateRange(1, 1000)][int]$DisplayLimit = 50,
    [string]$QueryFile
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot "ComposeWrapper.ps1")

function Fail([string]$Message) { throw "SPARK SQL CONSOLE FAILED: $Message" }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fail "Docker CLI is not on PATH." }
& docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop is not running or reachable." }
if (Get-Command docker-compose -ErrorAction SilentlyContinue) { $compose = "docker-compose"; $prefix = @() }
elseif ((& docker compose version 2>$null)) { $compose = "docker"; $prefix = @("compose") }
else { Fail "Neither docker-compose nor docker compose is available." }

$script:ComposeExitCode = 1
function Compose { Invoke-DemoCompose -Executable $script:compose -Prefix $script:prefix -ExitCode ([ref]$script:ComposeExitCode) -Arguments $args }
foreach ($service in @("spark-master", "spark-worker")) {
    $id = @(Compose ps -q $service | Where-Object { $_ -match '^[0-9a-f]{12,}$' })[0]
    if ($script:ComposeExitCode -ne 0 -or -not $id) { Fail "$service is not running. Start it with: docker compose up -d spark-master spark-worker" }
    if (-not ((& docker inspect -f '{{.State.Status}}' $id).Trim() -eq "running")) { Fail "$service is not running." }
}

$extra = @()
if ($QueryFile) {
    $full = (Resolve-Path $QueryFile -ErrorAction Stop).Path
    $root = (Split-Path $PSScriptRoot -Parent).TrimEnd('\')
    if (-not $full.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) { Fail "QueryFile must be inside the repository." }
    $relative = $full.Substring($root.Length).TrimStart('\').Replace('\', '/')
    $extra = @("--file", "/workspace/$relative")
}

$submit = @("--profile", "jobs", "run", "--interactive", "--rm", "--no-deps", "--entrypoint", "python3", "analytics",
    "/workspace/demo/spark_sql_repl.py", "--master", "spark://spark-master:7077",
    "--parquet", "/workspace/data/processed/unsw_nb15", "--display-limit", "$DisplayLimit") + $extra

Write-Host "Starting persistent Spark SQL console. Type HELP or EXIT." -ForegroundColor Cyan
$previousErrorAction = $ErrorActionPreference
try {
    # Do not use the output-capturing wrapper here: an interactive terminal needs
    # native stdin/stdout. Continue only during this native invocation.
    $ErrorActionPreference = "Continue"
    & $compose @prefix @submit
    $exitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousErrorAction
}
if ($exitCode -eq 130) { Write-Host "Spark SQL console interrupted." -ForegroundColor Yellow; exit 130 }
if ($exitCode -ne 0) { Fail "Spark SQL console exited with code $exitCode. Check Spark Master/Worker logs." }
