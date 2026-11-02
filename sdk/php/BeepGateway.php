<?php
/**
 * BeepGateway.php — PHP client for BEEP backends via the beepgw HTTP gateway.
 * Pure PHP (stream wrappers): no curl, no FFI. For native BEEP use bindings/php (ext-ffi).
 *
 *   require 'BeepGateway.php';
 *   $gw = new Minta\Beep\Gateway('http://127.0.0.1:8080');
 *   $r  = $gw->call('POST', '/orders', ['Content-Type' => 'application/json'], '{"id":7}');
 *   echo $r['status'], ' ', $r['body'];
 */
declare(strict_types=1);

namespace Minta\Beep;

final class GatewayException extends \RuntimeException
{
    public int $beepCode;
    public int $httpStatus;

    public function __construct(int $beepCode, int $httpStatus, string $message)
    {
        parent::__construct($message, $beepCode);
        $this->beepCode = $beepCode;
        $this->httpStatus = $httpStatus;
    }
}

final class Gateway
{
    private string $base;
    private ?string $upstream;

    public function __construct(?string $base = null, ?string $upstream = null)
    {
        $this->base = rtrim($base ?? (getenv('BEEP_GATEWAY') ?: 'http://127.0.0.1:8080'), '/');
        $this->upstream = $upstream;
    }

    /**
     * @param array<string,string> $headers
     * @return array{status:int, headers:array<string,string>, body:string}
     */
    public function call(string $method, string $target, array $headers = [], string $body = '', int $timeoutMs = 0): array
    {
        if ($this->upstream !== null) {
            $headers['X-Beep-Upstream'] = $this->upstream;
        }
        if ($timeoutMs > 0) {
            $headers['X-Beep-Timeout-Ms'] = (string)$timeoutMs;
        }
        $lines = [];
        foreach ($headers as $k => $v) {
            $lines[] = "$k: $v";
        }
        $ctx = stream_context_create(['http' => [
            'method' => strtoupper($method),
            'header' => implode("\r\n", $lines),
            'content' => $body,
            'ignore_errors' => true,
            'protocol_version' => 1.1,
            'timeout' => max(60, $timeoutMs / 1000 + 5),
        ]]);
        $data = @file_get_contents($this->base . $target, false, $ctx);
        if ($data === false) {
            throw new GatewayException(421, 0, 'gateway unreachable: ' . $this->base);
        }
        $lines = function_exists('http_get_last_response_headers')
            ? (http_get_last_response_headers() ?? [])
            : ($http_response_header ?? []);
        $status = 0;
        $out = [];
        foreach ($lines as $line) {
            if (preg_match('#^HTTP/\S+\s+(\d{3})#', $line, $m)) {
                $status = (int)$m[1];
                $out = [];
            } elseif (($p = strpos($line, ':')) !== false) {
                $out[strtolower(trim(substr($line, 0, $p)))] = trim(substr($line, $p + 1));
            }
        }
        if (isset($out['x-beep-error-code'])) {
            $msg = json_decode($data, true)['error']['message'] ?? $data;
            throw new GatewayException((int)$out['x-beep-error-code'], $status, (string)$msg);
        }
        return ['status' => $status, 'headers' => $out, 'body' => $data];
    }
}
