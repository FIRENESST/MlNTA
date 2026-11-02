<?php
/**
 * Beep.php — PHP 7.4+/8.x binding to libbeep via ext-ffi (native BEEP, no gateway).
 *
 *   require 'Beep.php';
 *   $c = new Minta\Beep\Client('127.0.0.1:10288');
 *   $r = $c->call('POST', '/orders', ['Content-Type' => 'application/json'], '{"id":7}');
 *   echo $r->status, ' ', $r->body;
 *
 * Needs `extension=ffi` (and `ffi.enable=true` outside the CLI). Library path: BEEP_LIB env
 * or beep.dll / libbeep.so / libbeep.dylib. Without FFI, use the gateway SDK in sdk/php.
 */
declare(strict_types=1);

namespace Minta\Beep;

final class BeepException extends \RuntimeException
{
    public int $rc;
    public int $beepCode;

    public function __construct(int $rc, int $beepCode, string $message)
    {
        parent::__construct($message, $rc);
        $this->rc = $rc;
        $this->beepCode = $beepCode;
    }
}

final class Response
{
    public int $status;
    /** @var array<int, array{0:string,1:string}> */
    public array $headers;
    public string $body;

    public function __construct(int $status, array $headers, string $body)
    {
        $this->status = $status;
        $this->headers = $headers;
        $this->body = $body;
    }

    public function header(string $name): ?string
    {
        foreach ($this->headers as [$k, $v]) {
            if (strcasecmp($k, $name) === 0) {
                return $v;
            }
        }
        return null;
    }
}

final class Client
{
    public const E_REMOTE = -3;
    private static ?\FFI $ffi = null;
    /** @var \FFI\CData */
    private $handle;

    public static function lib(): \FFI
    {
        if (self::$ffi === null) {
            $path = getenv('BEEP_LIB') ?: (PHP_OS_FAMILY === 'Windows' ? 'beep.dll' : (PHP_OS_FAMILY === 'Darwin' ? 'libbeep.dylib' : 'libbeep.so'));
            self::$ffi = \FFI::cdef(<<<'C'
                typedef struct beep_client beep_client;
                typedef struct beep_response beep_response;
                beep_client *beep_client_new(const char *address, int sessions, int max_channels, int timeout_ms);
                beep_client *beep_client_new_tls(const char *address, int sessions, int max_channels, int timeout_ms, const char *server_name, const char *ca_file);
                void beep_client_free(beep_client *client);
                int beep_call(beep_client *client, const char *method, const char *target, const char *headers,
                              const char *body, size_t body_len, beep_response **out);
                int beep_response_status(const beep_response *resp);
                const uint8_t *beep_response_body(const beep_response *resp, size_t *len);
                const char *beep_response_headers(const beep_response *resp);
                void beep_response_free(beep_response *resp);
                const char *beep_last_error(void);
                int beep_last_error_code(void);
                C, $path);
        }
        return self::$ffi;
    }

    public function __construct(string $address, int $sessions = 1, int $maxChannels = 64, int $timeoutMs = 30000,
                                ?string $tlsServerName = null, ?string $tlsCa = null)
    {
        $f = self::lib();
        $this->handle = $tlsServerName === null
            ? $f->beep_client_new($address, $sessions, $maxChannels, $timeoutMs)
            : $f->beep_client_new_tls($address, $sessions, $maxChannels, $timeoutMs, $tlsServerName, $tlsCa);
        if (\FFI::isNull($this->handle)) {
            throw new BeepException(-1, $f->beep_last_error_code(), (string)$f->beep_last_error());
        }
    }

    public function __destruct()
    {
        if ($this->handle !== null && !\FFI::isNull($this->handle)) {
            self::lib()->beep_client_free($this->handle);
        }
    }

    /** @param array<string,string>|array<int,array{0:string,1:string}> $headers */
    public function call(string $method, string $target, array $headers = [], string $body = ''): Response
    {
        $f = self::lib();
        $lines = [];
        foreach ($headers as $k => $v) {
            $lines[] = is_array($v) ? "{$v[0]}: {$v[1]}" : "$k: $v";
        }
        $out = $f->new('beep_response*');
        $rc = $f->beep_call($this->handle, $method, $target, implode("\n", $lines), $body, strlen($body), \FFI::addr($out));
        if ($rc !== 0) {
            throw new BeepException($rc, $f->beep_last_error_code(), (string)$f->beep_last_error());
        }
        try {
            $len = $f->new('size_t');
            $ptr = $f->beep_response_body($out, \FFI::addr($len));
            $data = $len->cdata > 0 ? \FFI::string($ptr, $len->cdata) : '';
            $hs = [];
            foreach (explode("\r\n", (string)$f->beep_response_headers($out)) as $line) {
                $p = strpos($line, ':');
                if ($p !== false) {
                    $hs[] = [trim(substr($line, 0, $p)), trim(substr($line, $p + 1))];
                }
            }
            return new Response($f->beep_response_status($out), $hs, $data);
        } finally {
            $f->beep_response_free($out);
        }
    }
}
