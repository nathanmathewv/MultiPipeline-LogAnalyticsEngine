param(
    [string]$SourceDir = "$env:USERPROFILE\Downloads",
    [string]$DataDir = "$(Split-Path -Parent $PSScriptRoot)\data\raw"
)

$ErrorActionPreference = "Stop"

$files = @(
    "NASA_access_log_Jul95.gz",
    "NASA_access_log_Aug95.gz"
)

New-Item -ItemType Directory -Path $DataDir -Force | Out-Null

foreach ($file in $files) {
    $source = Join-Path $SourceDir $file
    if (-not (Test-Path -LiteralPath $source)) {
        throw "Missing dataset file: $source"
    }

    $destination = Join-Path $DataDir $file
    Copy-Item -LiteralPath $source -Destination $destination -Force
    Write-Host "Copied $file to $DataDir"
}

Write-Host "Dataset ready. The ETL app can read these .gz files directly."
