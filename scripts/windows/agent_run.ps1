<#
.SYNOPSIS
Builds and launches the Windows agent from a PowerShell entry point.

.DESCRIPTION
This helper validates the Rust workspace layout, optionally clears any dangling
agent instance, rebuilds `overlord-agent-emule`, and then launches the agent in
either standard or attach-friendly debug mode.
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'debug', 'stop')]
    [string]$Command = 'start',

    [string]$ConfigPath,

    [switch]$DontKill,

    [string[]]$AgentArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:AgentProcessName = 'overlord-agent-emule'
$script:Paths = @{
    AgentsDir = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
    CargoToml = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\Cargo.toml'))
    AgentExe  = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\target\debug\overlord-agent-emule.exe'))
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
