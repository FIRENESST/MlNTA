<?php
// php test_gateway.php   (BEEP_GATEWAY env var)
require __DIR__ . '/BeepGateway.php';
use Minta\Beep\Gateway;
use Minta\Beep\GatewayException;
$fail = 0;
function check(bool $c, string $m): void { global $fail; if (!$c) { $fail++; echo "FAIL: $m\n"; } }
$gw = new Gateway();
$r = $gw->call('POST', '/echo', ['Content-Type' => 'text/plain', 'X-Test' => 'php'], 'hello from php');
check($r['status'] === 200 && $r['body'] === 'hello from php' && $r['headers']['x-test'] === 'php', 'echo ' . json_encode($r));
$r = $gw->call('GET', '/stream/3');
check($r['body'] === "line 0\nline 1\nline 2\n", 'stream');
try { $gw->call('GET', '/err/537'); check(false, 'expected exception'); }
catch (GatewayException $e) { check($e->beepCode === 537 && $e->httpStatus === 403, "err {$e->beepCode}"); }
$r = $gw->call('GET', '/status/404');
check($r['status'] === 404, 'status');
echo $fail ? "PHP: $fail FAILURE(S)\n" : "PHP (gateway): all tests passed\n";
exit($fail ? 1 : 0);
