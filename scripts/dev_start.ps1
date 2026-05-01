$ErrorActionPreference = "Stop"

chcp 65001 > $null
[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

$env:PATH = "$repoRoot;$env:PATH"

$backendURL = "http://127.0.0.1:8080"
$statusURL = "$backendURL/api/status"
$goOutLog = Join-Path $repoRoot "flow\dev-go.out.log"
$goErrLog = Join-Path $repoRoot "flow\dev-go.err.log"
New-Item -ItemType Directory -Force -Path (Split-Path $goOutLog) | Out-Null

Write-Host "starting go backend..."
$go = Start-Process `
  -FilePath "go" `
  -ArgumentList @("run", ".", "-config", ".\config\config.json", "-no-open") `
  -WorkingDirectory $repoRoot `
  -NoNewWindow `
  -RedirectStandardOutput $goOutLog `
  -RedirectStandardError $goErrLog `
  -PassThru

$deadline = (Get-Date).AddSeconds(180)
$ready = $false
while ((Get-Date) -lt $deadline) {
  if ($go.HasExited) {
    $tail = ""
    if (Test-Path $goErrLog) {
      $tail = (Get-Content -Tail 80 -Encoding UTF8 $goErrLog) -join "`n"
    }
    if ($tail -eq "" -and (Test-Path $goOutLog)) {
      $tail = (Get-Content -Tail 80 -Encoding UTF8 $goOutLog) -join "`n"
    }
    throw "go backend exited before ready. log:`n$tail"
  }
  try {
    $resp = Invoke-WebRequest -Uri $statusURL -UseBasicParsing -TimeoutSec 2
    if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
      $ready = $true
      break
    }
  } catch {
    Start-Sleep -Milliseconds 500
  }
}

if (-not $ready) {
  Stop-Process -Id $go.Id -Force -ErrorAction SilentlyContinue
  $tail = ""
  if (Test-Path $goErrLog) {
    $tail = (Get-Content -Tail 80 -Encoding UTF8 $goErrLog) -join "`n"
  }
  if ($tail -eq "" -and (Test-Path $goOutLog)) {
    $tail = (Get-Content -Tail 80 -Encoding UTF8 $goOutLog) -join "`n"
  }
  throw "go backend was not ready within 180 seconds. log:`n$tail"
}

Write-Host "start success."
Write-Host "starting frontend..."
npm --prefix web run dev:open
