function Invoke-DemoCompose {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Executable,
        [string[]]$Prefix = @(),
        [Parameter(Mandatory)][ref]$ExitCode,
        [Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments
    )

    # Docker forwards Hadoop stderr.  Under ErrorActionPreference=Stop, Windows
    # PowerShell can surface an exit-0 native warning as a terminating error.
    # Keep this relaxation local, preserve every stream as text, and expose the
    # real native exit code for the caller to validate.
    $previousErrorAction = $ErrorActionPreference
    $ExitCode.Value = 1
    try {
        $ErrorActionPreference = "Continue"
        & $Executable @Prefix @Arguments 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() }
            else { [string]$_ }
        }
        $nativeExitCode = $LASTEXITCODE
        if ($null -ne $nativeExitCode) { $ExitCode.Value = [int]$nativeExitCode }
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
}
