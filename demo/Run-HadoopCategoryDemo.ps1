[CmdletBinding()]
param(
    [string]$InputDir = "/data/unsw/raw"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Fail([string]$Message) { throw "DEMO FAILED: $Message" }
function Format-Count([Int64]$Value) { return $Value.ToString("N0", [Globalization.CultureInfo]::InvariantCulture) }
. (Join-Path $PSScriptRoot "ComposeWrapper.ps1")

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail "Docker CLI is not installed or not on PATH."
}
& docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop is not running or is not reachable." }

if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    $compose = "docker-compose"; $composePrefix = @()
} elseif ((& docker compose version 2>$null)) {
    $compose = "docker"; $composePrefix = @("compose")
} else {
    Fail "Neither docker-compose nor docker compose is available."
}
$script:LastComposeExitCode = 1

function Compose {
    # Deliberately use $args: Hadoop arguments such as -e and -test must pass
    # through unchanged instead of being interpreted as PowerShell parameters.
    Invoke-DemoCompose -Executable $script:compose -Prefix $script:composePrefix -ExitCode ([ref]$script:LastComposeExitCode) -Arguments $args
}

Write-Host "Checking Hadoop services..." -ForegroundColor Cyan
foreach ($service in @("namenode", "datanode", "resourcemanager", "nodemanager")) {
    $id = @(Compose ps -q $service | Where-Object { $_ -match '^[0-9a-f]{12,}$' })[0]
    if ($script:LastComposeExitCode -ne 0) { Fail "Could not inspect Compose service $service." }
    if (-not $id) { Fail "$service is not running. Start the Hadoop profile first." }
    $state = (& docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' $id).Trim()
    if (-not $state.StartsWith("running")) { Fail "$service is not ready: $state" }
}

$report = @(Compose exec -T namenode hdfs dfsadmin -report 2>&1)
if ($script:LastComposeExitCode -ne 0 -or -not (($report -join "`n") -match "Live datanodes \(1\):")) {
    Fail "HDFS is not ready with one live DataNode."
}
$nodes = @(Compose exec -T resourcemanager yarn node -list 2>&1)
if ($script:LastComposeExitCode -ne 0 -or -not (($nodes -join "`n") -match "RUNNING")) {
    Fail "YARN has no RUNNING NodeManager."
}

$listing = @(Compose exec -T namenode hdfs dfs -ls $InputDir 2>&1)
if ($script:LastComposeExitCode -ne 0) { Fail "Cannot list HDFS input directory $InputDir." }
$files = @($listing | Where-Object { $_ -match 'UNSW-NB15_[1-4]\.csv$' })
if ($files.Count -ne 4) { Fail "Expected four original UNSW-NB15 CSVs in $InputDir; found $($files.Count)." }

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$outputDir = "/data/unsw/demo/category-$stamp-$PID"
Compose exec -T namenode hdfs dfs -test -e $outputDir
if ($script:LastComposeExitCode -eq 0) { Fail "Refusing to reuse existing demo output $outputDir." }

Write-Host "HDFS input: $InputDir (4 files)" -ForegroundColor Green
Write-Host "Demo output: $outputDir" -ForegroundColor Green
Write-Host "Compiling existing UnswMapReduce.java and submitting unsw-category..." -ForegroundColor Cyan

$jobLines = New-Object System.Collections.Generic.List[string]
# Use Bash explicitly. The mounted script keeps command substitution and quoting
# inside Bash; PowerShell passes only two HDFS paths as separate arguments.
Compose --profile hadoop run --rm --no-deps --entrypoint bash hadoop-jobs /workspace/demo/run_hadoop_category.sh $InputDir $outputDir | ForEach-Object {
    $line = $_.ToString(); [void]$jobLines.Add($line); Write-Host $line
}
if ($script:LastComposeExitCode -ne 0) { Fail "The category job failed. HDFS output was left for diagnosis at $outputDir." }

$application = ($jobLines | Select-String -AllMatches 'application_\d+_\d+' | ForEach-Object Matches | ForEach-Object Value | Select-Object -Last 1)
if ($application) { Write-Host "YARN application: $application" -ForegroundColor Green }
else { Write-Warning "Job succeeded but its application ID was not found in client output; see YARN UI."
}

Compose exec -T namenode hdfs dfs -test -e "$outputDir/_SUCCESS"
if ($script:LastComposeExitCode -ne 0) { Fail "Job returned success but HDFS _SUCCESS is missing at $outputDir." }
$rawLines = @(Compose exec -T namenode hdfs dfs -cat "$outputDir/part-r-*")
if ($script:LastComposeExitCode -ne 0 -or $rawLines.Count -eq 0) { Fail "No category result records were produced." }
$knownHadoopWarning = '^WARNING: log4j\.properties is not found\. HADOOP_CONF_DIR may be incomplete\.$'
$lines = @($rawLines | Where-Object { $_ -notmatch $knownHadoopWarning })

$counts = @{}
foreach ($line in $lines) {
    $parts = $line -split "`t", 2
    if ($parts.Count -ne 2 -or $parts[1] -notmatch '^\d+$') { Fail "Malformed category output: $line" }
    $counts[$parts[0]] = [Int64]$parts[1]
}
$total = [Int64](($counts.Values | Measure-Object -Sum).Sum)
$normal = [Int64]$counts["Normal"]
$attacks = $total - $normal

Write-Host "`nGenerated attack-category counts:" -ForegroundColor Cyan
$counts.GetEnumerator() | Sort-Object Name | ForEach-Object { "{0,-12} {1,12}" -f $_.Name, (Format-Count $_.Value) } | Write-Host
Write-Host ("`nReconciliation: total={0}; normal={1}; observed attacks={2}" -f (Format-Count $total), (Format-Count $normal), (Format-Count $attacks)) -ForegroundColor Cyan
if ($total -ne 2540047 -or $normal -ne 2218764 -or $attacks -ne 321283) {
    Fail "Unexpected totals. Expected total=2540047, normal=2218764, attacks=321283."
}
Write-Host "DEMO PASSED: results reconcile. Existing /data/unsw/out/category was not modified." -ForegroundColor Green
Write-Host "Demo output retained at $outputDir for the presentation; delete it manually only after review." -ForegroundColor Yellow
