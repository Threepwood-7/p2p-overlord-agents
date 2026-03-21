$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$agentsDir = Resolve-Path (Join-Path $scriptDir "..\..")

Push-Location $agentsDir
try {
    Write-Host "Running cargo fmt --all --check..."
    & cargo fmt --all --check
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    $clippyArgs = @(
        "clippy",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--",
        "-D",
        "warnings",
        "-W",
        "clippy::all"
    )

    Write-Host "Running cargo $($clippyArgs -join ' ')..."
    & cargo @clippyArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}
