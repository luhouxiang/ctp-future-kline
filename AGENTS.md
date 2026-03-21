# Repository Instructions

## Test Environment

- Before running `go test`, `dlv debug`, or any debug/test process that loads CTP dynamic libraries, prepend the repository root to `PATH` in the same shell session:
  ```powershell
  $env:PATH = "E:\work\go\ctp-future-kline;$env:PATH"
  ```
- Use the same `PATH` preparation for related commands such as `go test ./...`, `go test ./internal/trade`, `dlv debug`, and `dlv test`, so tests do not fail due to missing `thostmduserapi_se.dll`, `thosttraderapi_se.dll`, or `wrap.dll`.
