# Beep.psm1 — PowerShell 7+ client for BEEP backends via the beepgw HTTP gateway.
#
#   Import-Module ./Beep.psm1
#   $r = Invoke-Beep -Method POST -Target /orders -Body '{"id":7}' -ContentType application/json
#   $r.Status; $r.Body; $r.Headers['X-Request-Id']
#
# For native BEEP without the gateway, load the .NET library instead:
#   Add-Type -Path Minta.Beep.dll; $c = [Minta.Beep.RpcClient]::new('127.0.0.1', 10288, $null)

$script:Gateway = if ($env:BEEP_GATEWAY) { $env:BEEP_GATEWAY } else { 'http://127.0.0.1:8080' }
$script:Http = [System.Net.Http.HttpClient]::new()

class BeepError : System.Exception {
    [int]$Code
    [int]$HttpStatus
    BeepError([int]$code, [int]$status, [string]$message) : base($message) { $this.Code = $code; $this.HttpStatus = $status }
}

function Set-BeepGateway([Parameter(Mandatory)][string]$Url) { $script:Gateway = $Url.TrimEnd('/') }

function Invoke-Beep {
    [CmdletBinding()]
    param(
        [string]$Method = 'GET',
        [Parameter(Mandatory)][string]$Target,
        [hashtable]$Headers = @{},
        $Body = $null,
        [string]$ContentType,
        [string]$Upstream,
        [int]$TimeoutMs = 0,
        [switch]$NoThrow
    )
    $req = [System.Net.Http.HttpRequestMessage]::new([System.Net.Http.HttpMethod]::new($Method.ToUpperInvariant()), "$script:Gateway$Target")
    if ($null -ne $Body) {
        $bytes = if ($Body -is [byte[]]) { $Body } else { [System.Text.Encoding]::UTF8.GetBytes([string]$Body) }
        $req.Content = [System.Net.Http.ByteArrayContent]::new($bytes)
        if ($ContentType) { $req.Content.Headers.TryAddWithoutValidation('Content-Type', $ContentType) | Out-Null }
    }
    foreach ($k in $Headers.Keys) {
        if (-not $req.Headers.TryAddWithoutValidation($k, [string]$Headers[$k]) -and $req.Content) {
            $req.Content.Headers.TryAddWithoutValidation($k, [string]$Headers[$k]) | Out-Null
        }
    }
    if ($Upstream) { $req.Headers.Add('X-Beep-Upstream', $Upstream) }
    if ($TimeoutMs -gt 0) { $req.Headers.Add('X-Beep-Timeout-Ms', [string]$TimeoutMs) }

    $resp = $script:Http.SendAsync($req).GetAwaiter().GetResult()
    $raw = $resp.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
    $h = @{}
    foreach ($kv in $resp.Headers) { $h[$kv.Key] = ($kv.Value -join ', ') }
    foreach ($kv in $resp.Content.Headers) { $h[$kv.Key] = ($kv.Value -join ', ') }
    $result = [pscustomobject]@{
        Status   = [int]$resp.StatusCode
        Headers  = $h
        Body     = [System.Text.Encoding]::UTF8.GetString($raw)
        RawBody  = $raw
    }
    $beepCode = $h['X-Beep-Error-Code']
    if ($beepCode -and -not $NoThrow) {
        $msg = try { ($result.Body | ConvertFrom-Json).error.message } catch { $result.Body }
        throw [BeepError]::new([int]$beepCode, $result.Status, $msg)
    }
    $result
}

Export-ModuleMember -Function Invoke-Beep, Set-BeepGateway
