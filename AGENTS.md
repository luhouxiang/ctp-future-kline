# Repository Instructions

## Test Environment

- Before running `go test`, `dlv debug`, or any debug/test process that loads CTP dynamic libraries, prepend the repository root to `PATH` in the same shell session:
  ```powershell
  $env:PATH = "E:\work\go\ctp-future-kline;$env:PATH"
  ```
- Use the same `PATH` preparation for related commands such as `go test ./...`, `go test ./internal/trade`, `dlv debug`, and `dlv test`, so tests do not fail due to missing `thostmduserapi_se.dll`, `thosttraderapi_se.dll`, or `wrap.dll`.

## PowerShell UTF-8 Safety

- Before using PowerShell to read, transform, or write any repo-tracked text file, initialize the shell to UTF-8 in the same session:
  ```powershell
  chcp 65001 > $null
  [Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
  [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
  $OutputEncoding = [System.Text.UTF8Encoding]::new($false)
  ```
- Do not rely on PowerShell or `cmd` default encodings when editing files that may contain Chinese text.
- When writing text files from PowerShell, always use explicit UTF-8 output, preferably without BOM, for example:
  ```powershell
  $text = Get-Content -Raw -Encoding UTF8 $path
  [System.IO.File]::WriteAllText($path, $text, [System.Text.UTF8Encoding]::new($false))
  ```
- Avoid using `>`, `>>`, `Out-File`, `Set-Content`, or `Add-Content` for repo-tracked source files unless the command explicitly sets UTF-8 encoding.
