# Godis PowerShell Build Script
# Usage: .\build.ps1 [-Output <path>] [-Target <arch>]
# 
# Examples:
#   .\build.ps1                          # Build godis.exe for current architecture
#   .\build.ps1 -Output mygodis.exe      # Build with custom output name
#   .\build.ps1 -Target x64              # Build for x64 (amd64)
#   .\build.ps1 -Target x86              # Build for x86 (386)

param(
    [string]$Output = "godis.exe",
    [ValidateSet("x64", "x86", "amd64", "386", "arm64")]
    [string]$Target = ""
)

# Set GOARCH based on Target parameter
$env:CGO_ENABLED = "0"

if ($Target) {
    switch ($Target) {
        "x64" { $env:GOARCH = "amd64" }
        "x86" { $env:GOARCH = "386" }
        default { $env:GOARCH = $Target }
    }
}

$env:GOOS = "windows"

Write-Host "Building Godis for Windows..." -ForegroundColor Cyan
if ($env:GOARCH) {
    Write-Host "Target Architecture: $env:GOARCH" -ForegroundColor Gray
} else {
    Write-Host "Target Architecture: default (current)" -ForegroundColor Gray
}
Write-Host "Output: $Output" -ForegroundColor Gray
Write-Host ""

go build -o $Output .
if ($LASTEXITCODE -eq 0) {
    Write-Host "Build successful!" -ForegroundColor Green
    Write-Host "Output: $(Resolve-Path $Output)" -ForegroundColor Gray
} else {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}
