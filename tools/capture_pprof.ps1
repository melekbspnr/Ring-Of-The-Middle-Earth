param(
    [string]$Port = "8080",
    [string]$Profile = "goroutine",
    [int]$Debug = 1,
    [string]$OutFile = "artifacts/pprof-goroutine.txt"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$outputPath = if ([System.IO.Path]::IsPathRooted($OutFile)) {
    $OutFile
} else {
    Join-Path $repoRoot $OutFile
}

$outputDir = Split-Path -Parent $outputPath
if ($outputDir -and -not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

$uri = "http://localhost:$Port/debug/pprof/${Profile}?debug=$Debug"
Invoke-WebRequest -Uri $uri -OutFile $outputPath -ErrorAction Stop
Write-Host "Saved $uri -> $outputPath"
