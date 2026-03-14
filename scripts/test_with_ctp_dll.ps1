param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$GoTestArgs = @("./...")
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$required = @(
  "wrap.dll",
  "thosttraderapi_se.dll",
  "thostmduserapi_se.dll"
)

$missing = @()
foreach ($name in $required) {
  $path = Join-Path $repoRoot $name
  if (-not (Test-Path $path -PathType Leaf)) {
    $missing += $name
  }
}

if ($missing.Count -gt 0) {
  throw "Missing CTP DLLs in repo root: $($missing -join ', ')"
}

$env:PATH = "$repoRoot;$env:PATH"
Write-Host "PATH prepended with $repoRoot"
Write-Host "Running: go test $($GoTestArgs -join ' ')"

go test @GoTestArgs
