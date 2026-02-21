param(
  [string]$DbPath = "flow/future_kline.db",
  [string]$CsvPath = "",
  [string]$Url = ""
)

if ([string]::IsNullOrWhiteSpace($CsvPath) -and [string]::IsNullOrWhiteSpace($Url)) {
  Write-Error "Usage: ./scripts/import_trading_calendar.ps1 -CsvPath <calendar.csv> [-DbPath flow/future_kline.db] OR -Url <https://...>"
  exit 2
}

$args = @("run", "./cmd/calendar_import", "-db", $DbPath)
if (-not [string]::IsNullOrWhiteSpace($CsvPath)) {
  $args += @("-source", "csv")
  $args += @("-csv", $CsvPath)
}
if (-not [string]::IsNullOrWhiteSpace($Url)) {
  if ([string]::IsNullOrWhiteSpace($CsvPath)) {
    $args += @("-source", "url")
  }
  $args += @("-url", $Url)
}

go @args
exit $LASTEXITCODE
