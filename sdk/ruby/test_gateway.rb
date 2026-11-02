# ruby test_gateway.rb   (BEEP_GATEWAY env var)
require_relative 'beep_gateway'
$fail = 0
def check(cond, msg)
  return if cond

  $fail += 1
  puts "FAIL: #{msg}"
end
gw = Beep::Gateway.new
r = gw.call('POST', '/echo', headers: { 'Content-Type' => 'text/plain', 'X-Test' => 'ruby' }, body: 'hello from ruby')
check(r.status == 200 && r.body == 'hello from ruby' && r.headers['x-test'] == 'ruby', "echo #{r.inspect}")
r = gw.call('GET', '/stream/3')
check(r.body == "line 0\nline 1\nline 2\n", 'stream')
begin
  gw.call('GET', '/err/537')
  check(false, 'expected error')
rescue Beep::GatewayError => e
  check(e.code == 537 && e.status == 403, "err #{e.code}")
end
check(gw.call('GET', '/status/404').status == 404, 'status')
puts $fail.zero? ? 'Ruby (gateway): all tests passed' : "Ruby: #{$fail} FAILURE(S)"
exit($fail.zero? ? 0 : 1)
