<#
.SYNOPSIS
Builds and launches the Windows agent from a PowerShell entry point.

.DESCRIPTION
This helper validates the Rust workspace layout, optionally clears any dangling
agent instance, rebuilds `overlord-agent-emule`, and then launches the agent in
either standard or attach-friendly debug mode. Pass `-RunDetached` to relaunch
this helper in a minimized background PowerShell 7 window and return immediately.
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'debug', 'stop')]
    [string]$Command = 'start',

    [string]$ConfigPath,

    [switch]$DontKill,

    [string[]]$AgentArgs = @(),

    [switch]$RunDetached
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:InvocationBoundParameters = @{}
foreach ($entry in $PSBoundParameters.GetEnumerator()) {
    $script:InvocationBoundParameters[$entry.Key] = $entry.Value
}

$script:AgentProcessName = 'overlord-agent-emule'
$script:WorkspaceProjectDir = if ([string]::IsNullOrWhiteSpace($env:OVERLORD_PROJECT_DIR)) {
    [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
}
else {
    [System.IO.Path]::GetFullPath($env:OVERLORD_PROJECT_DIR)
}
$script:AgentsDir = [System.IO.Path]::GetFullPath((Join-Path $script:WorkspaceProjectDir 'overlord-agents'))
$script:Paths = @{
    AgentsDir = $script:AgentsDir
    CargoToml = Join-Path $script:AgentsDir 'Cargo.toml'
    AgentExe  = Join-Path $script:AgentsDir 'target\debug\overlord-agent-emule.exe'
}

function Write-Log {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    [Console]::Out.WriteLine($Message)
}

function Fail {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    throw $Message
}

function Assert-Windows {
    if (-not $IsWindows) {
        Fail "This helper currently supports Windows only. Detected platform: $([System.Environment]::OSVersion.Platform)"
    }
}

function Ensure-AgentsLayout {
    if (-not (Test-Path -LiteralPath $script:Paths.CargoToml -PathType Leaf)) {
        Fail "Agent workspace Cargo.toml is missing at $($script:Paths.CargoToml)"
    }
}

function Resolve-Pwsh7Path {
    $programFiles = if ([string]::IsNullOrWhiteSpace($env:ProgramFiles)) {
        'C:\Program Files'
    }
    else {
        $env:ProgramFiles
    }

    $preferredPath = Join-Path $programFiles 'PowerShell\7\pwsh.exe'
    if (Test-Path -LiteralPath $preferredPath -PathType Leaf) {
        return $preferredPath
    }

    $pwshCommand = Get-Command -Name 'pwsh.exe' -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -ne $pwshCommand -and -not [string]::IsNullOrWhiteSpace($pwshCommand.Source)) {
        return $pwshCommand.Source
    }

    Fail 'PowerShell 7 `pwsh.exe` was not found. Install PowerShell 7 and retry.'
}

function Get-DetachedInvocationArguments {
    # Build a deterministic argument list and intentionally omit -RunDetached to
    # avoid recursive relaunch loops.
    $forwardedArgs = @($Command)

    if ($script:InvocationBoundParameters.ContainsKey('ConfigPath')) {
        $forwardedArgs += '-ConfigPath'
        $forwardedArgs += $ConfigPath
    }

    if ($script:InvocationBoundParameters.ContainsKey('DontKill')) {
        if ($DontKill.IsPresent) {
            $forwardedArgs += '-DontKill'
        }
        else {
            $forwardedArgs += '-DontKill:$false'
        }
    }

    if ($script:InvocationBoundParameters.ContainsKey('AgentArgs') -and $AgentArgs.Count -gt 0) {
        $forwardedArgs += '-AgentArgs'
        $forwardedArgs += $AgentArgs
    }

    return $forwardedArgs
}

function Start-DetachedSelf {
    $pwshPath = Resolve-Pwsh7Path
    $scriptPath = [System.IO.Path]::GetFullPath($PSCommandPath)
    $argumentList = @('-NoLogo', '-NoProfile', '-File', $scriptPath)
    $argumentList += @(Get-DetachedInvocationArguments)

    $detachedProcess = Start-Process `
        -FilePath $pwshPath `
        -ArgumentList $argumentList `
        -WorkingDirectory (Get-Location).Path `
        -WindowStyle Minimized `
        -PassThru

    try {
        Write-Log "Detached agent helper started as PID $($detachedProcess.Id)."
    }
    finally {
        $detachedProcess.Dispose()
    }

    return 0
}

function Get-AgentRuntimeArguments {
    param(
        [AllowNull()]
        [string]$ResolvedConfigPath,

        [string[]]$AdditionalArgs = @()
    )

    $runtimeArgs = @()
    if (-not [string]::IsNullOrWhiteSpace($ResolvedConfigPath)) {
        $runtimeArgs += '--config'
        $runtimeArgs += $ResolvedConfigPath
    }

    if ($AdditionalArgs.Count -gt 0) {
        $runtimeArgs += $AdditionalArgs
    }

    return $runtimeArgs
}

function Get-DanglingAgentProcesses {
    # For the current workflow we only expect one live agent at a time, so any
    # existing `overlord-agent-emule` process is treated as stale by default.
    return @(Get-Process -Name $script:AgentProcessName -ErrorAction SilentlyContinue)
}

function Stop-DanglingAgentProcesses {
    $danglingProcesses = @(Get-DanglingAgentProcesses)
    if ($danglingProcesses.Count -eq 0) {
        return
    }

    Write-Log "Found dangling agent instance(s): $($danglingProcesses.Id -join ', ')"
    foreach ($process in $danglingProcesses) {
        Write-Log "Hard-killing agent process $($process.Id)..."
        Stop-Process -Id $process.Id -Force
    }

    $danglingProcesses |
        Select-Object -ExpandProperty Id |
        ForEach-Object { Wait-Process -Id $_ -Timeout 5 -ErrorAction SilentlyContinue }
}

function Stop-Agent {
    $runningProcesses = @(Get-DanglingAgentProcesses)
    if ($runningProcesses.Count -eq 0) {
        Write-Log 'No overlord-agent-emule process is currently running.'
        return 0
    }

    Write-Log "Stopping overlord-agent-emule instance(s): $($runningProcesses.Id -join ', ')"
    foreach ($process in $runningProcesses) {
        Stop-Process -Id $process.Id -Force
    }

    $runningProcesses |
        Select-Object -ExpandProperty Id |
        ForEach-Object { Wait-Process -Id $_ -Timeout 5 -ErrorAction SilentlyContinue }

    Write-Log 'overlord-agent-emule stopped.'
    return 0
}

function Invoke-CargoBuild {
    Write-Log 'Rebuilding overlord-agent-emule...'

    Push-Location -LiteralPath $script:Paths.AgentsDir
    try {
        & cargo build -p overlord-agent-emule --bin overlord-agent-emule
        return $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
}

function Test-AgentExecutable {
    if (-not (Test-Path -LiteralPath $script:Paths.AgentExe -PathType Leaf)) {
        Fail "Built agent executable is missing at $($script:Paths.AgentExe)"
    }
}

function Start-AgentProcess {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('start', 'debug')]
        [string]$LaunchMode,

        [string[]]$RuntimeArgs = @()
    )

    if ($LaunchMode -eq 'debug' -and [string]::IsNullOrWhiteSpace($env:RUST_BACKTRACE)) {
        # Provide better crash diagnostics for debug launches without permanently
        # changing the caller's environment.
        $previousBacktrace = $null
        $hadPreviousBacktrace = $false
        if (Test-Path Env:\RUST_BACKTRACE) {
            $hadPreviousBacktrace = $true
            $previousBacktrace = $env:RUST_BACKTRACE
        }

        $env:RUST_BACKTRACE = '1'
        try {
            & $script:Paths.AgentExe @RuntimeArgs
            return $LASTEXITCODE
        }
        finally {
            if ($hadPreviousBacktrace) {
                $env:RUST_BACKTRACE = $previousBacktrace
            }
            else {
                Remove-Item Env:\RUST_BACKTRACE -ErrorAction SilentlyContinue
            }
        }
    }

    & $script:Paths.AgentExe @RuntimeArgs
    return $LASTEXITCODE
}

function Start-Agent {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('start', 'debug')]
        [string]$LaunchMode,

        [bool]$ShouldKillDanglingAgents = $true,

        [AllowNull()]
        [string]$ResolvedConfigPath,

        [string[]]$AdditionalArgs = @()
    )

    if ($ShouldKillDanglingAgents) {
        Stop-DanglingAgentProcesses
    }

    $buildExitCode = Invoke-CargoBuild
    if ($buildExitCode -ne 0) {
        return $buildExitCode
    }

    Test-AgentExecutable
    $runtimeArgs = @(Get-AgentRuntimeArguments -ResolvedConfigPath $ResolvedConfigPath -AdditionalArgs $AdditionalArgs)
    if ($LaunchMode -eq 'debug') {
        Write-Log 'Starting overlord-agent-emule in debug mode...'
    }
    else {
        Write-Log 'Starting overlord-agent-emule...'
    }

    Push-Location -LiteralPath $script:Paths.AgentsDir
    try {
        return Start-AgentProcess -LaunchMode $LaunchMode -RuntimeArgs $runtimeArgs
    }
    finally {
        Pop-Location
    }
}

function Invoke-Main {
    Assert-Windows
    Ensure-AgentsLayout

    if ($RunDetached.IsPresent) {
        return Start-DetachedSelf
    }

    if ($Command -eq 'stop') {
        return Stop-Agent
    }

    return Start-Agent `
        -LaunchMode $Command `
        -ShouldKillDanglingAgents (-not $DontKill.IsPresent) `
        -ResolvedConfigPath $ConfigPath `
        -AdditionalArgs $AgentArgs
}

try {
    exit (Invoke-Main)
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
