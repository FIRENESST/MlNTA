# ruby test_beep.rb 127.0.0.1:10288   (BEEP_LIB=/path/to/beep.dll)
require_relative 'beep'

$failures = 0
def check(cond, msg)
  return if cond

  $failures += 1
  puts "FAIL: #{msg}"
end

addr = ARGV[0] || '127.0.0.1:10288'
c = Beep::Client.new(addr, max_channels: 8, timeout_ms: 5000)
r = c.call('POST', '/echo', headers: { 'Content-Type' => 'text/plain', 'X-Test' => 'ruby' }, body: 'hello from ruby')
check(r.status == 200 && r.body == 'hello from ruby' && r['x-test'] == 'ruby', "echo #{r.inspect}")
r = c.get('/stream/3')
check(r.body == "line 0\nline 1\nline 2\n", "stream #{r.body.inspect}")
begin
  c.get('/err/537')
  check(false, 'expected Beep::Error')
rescue Beep::Error => e
  check(e.rc == Beep::E_REMOTE && e.code == 537, "err #{e.rc} #{e.code}")
end
bin = (0..255).map(&:chr).join.b * 10
r = c.call('PUT', '/echo', body: bin)
check(r.body == bin, 'binary-safe body')
threads = 8.times.map { |i| Thread.new { c.post('/sleep/30', i.to_s).status } }
check(threads.map(&:value).all?(200), 'threads')
puts $failures.zero? ? 'Ruby (FFI): all tests passed' : "Ruby (FFI): #{$failures} FAILURE(S)"
exit($failures.zero? ? 0 : 1)
