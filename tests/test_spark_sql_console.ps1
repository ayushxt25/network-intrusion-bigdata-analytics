$ErrorActionPreference = "Stop"
$runner = Join-Path $PSHOME "pwsh.exe"
if (-not (Test-Path $runner)) { throw "PowerShell runner is unavailable: $runner" }
$log = Join-Path ([IO.Path]::GetTempPath()) ("spark-sql-console-" + [guid]::NewGuid() + ".log")
try {
    & $runner -NoProfile -ExecutionPolicy Bypass -File "$PSScriptRoot\..\demo\Start-SparkSQL.ps1" -DisplayLimit 10 -QueryFile "$PSScriptRoot\fixtures\spark_sql_repl_integration.sql" *> $log
    if ($LASTEXITCODE -ne 0) { throw "Spark SQL console integration failed: $(Get-Content $log -Raw)" }
    $text = Get-Content $log -Raw
} finally {
    if (Test-Path $log) { Remove-Item -LiteralPath $log -Force }
}
foreach ($expected in @("Commands: HELP", "network_flows", "2540047", "321283", "2218764", "SQL ERROR:", "records_after_error")) {
    if ($text -notmatch [regex]::Escape($expected)) { throw "Missing expected console output: $expected" }
}
"SPARK_SQL_QUERY_TIMES=" + (($text | Select-String -AllMatches 'Execution time: ([0-9.]+) seconds' | ForEach-Object Matches | ForEach-Object { $_.Groups[1].Value }) -join ',')
"SPARK_SQL_CONSOLE_TEST_OK"
