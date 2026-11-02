<?php
// php -d extension=ffi test_beep.php 127.0.0.1:10288    (BEEP_LIB=/path/to/beep.dll)
require __DIR__ . '/Beep.php';

use Minta\Beep\BeepException;
use Minta\Beep\Client;

$failures = 0;
function check(bool $cond, string $msg): void
{
    global $failures;
    if (!$cond) {
        $failures++;
        echo "FAIL: $msg\n";
    }
}

$c = new Client($argv[1] ?? '127.0.0.1:10288', 1, 8, 5000);
$r = $c->call('POST', '/echo', ['Content-Type' => 'text/plain', 'X-Test' => 'php'], 'hello from php');
check($r->status === 200 && $r->body === 'hello from php' && $r->header('x-test') === 'php', 'echo ' . $r->status . ' ' . $r->body);
$r = $c->call('GET', '/stream/3');
check($r->body === "line 0\nline 1\nline 2\n", 'stream ' . json_encode($r->body));
try {
    $c->call('GET', '/err/550');
    check(false, 'expected exception');
} catch (BeepException $e) {
    check($e->rc === Client::E_REMOTE && $e->beepCode === 550, "err {$e->rc} {$e->beepCode}");
}
$bin = str_repeat(implode('', array_map('chr', range(0, 255))), 10);
$r = $c->call('PUT', '/echo', [], $bin);
check($r->body === $bin, 'binary-safe body');
echo $failures === 0 ? "PHP (FFI): all tests passed\n" : "PHP (FFI): $failures FAILURE(S)\n";
exit($failures === 0 ? 0 : 1);
