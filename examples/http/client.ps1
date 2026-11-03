$endpoint = if ($env:MINTA_GATEWAY) { $env:MINTA_GATEWAY } else { 'http://127.0.0.1:8080' }
$payload = @{ method = 'POST'; target = '/echo'; json = @{ hello = 'powershell' } } | ConvertTo-Json -Depth 5
$reply = Invoke-RestMethod -Uri "$endpoint/v1/call" -Method Post -ContentType 'application/json' -Body $payload -TimeoutSec 10 -NoProxy
if ($reply.status -ne 200 -or ($reply.body | ConvertFrom-Json).hello -ne 'powershell') { throw 'Unexpected response' }
'PowerShell HTTP/JSON: all tests passed'
