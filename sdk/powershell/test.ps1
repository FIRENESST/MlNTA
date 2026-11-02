# pwsh -File test.ps1   (BEEP_GATEWAY env var)
Import-Module (Join-Path $PSScriptRoot 'Beep.psm1') -Force
$fail = 0
function Check($cond, $msg) { if (-not $cond) { $script:fail++; Write-Host "FAIL: $msg" } }
$r = Invoke-Beep -Method POST -Target '/echo' -Body 'hello from powershell' -ContentType 'text/plain' -Headers @{ 'X-Test' = 'ps' }
Check ($r.Status -eq 200 -and $r.Body -eq 'hello from powershell' -and $r.Headers['x-test'] -eq 'ps') "echo $($r.Status) $($r.Body)"
$r = Invoke-Beep -Target '/stream/3'
Check ($r.Body -eq "line 0`nline 1`nline 2`n") "stream"
try { Invoke-Beep -Target '/err/537' | Out-Null; Check $false 'expected BeepError' }
catch { Check ($_.Exception.Code -eq 537 -and $_.Exception.HttpStatus -eq 403) "err $($_.Exception.Code)" }
$r = Invoke-Beep -Target '/status/404'
Check ($r.Status -eq 404) "status"
if ($fail -eq 0) { 'PowerShell (gateway): all tests passed' } else { "PowerShell: $fail FAILURE(S)"; exit 1 }
