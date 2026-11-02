# crystal run test.cr   (BEEP_GATEWAY env var)
require "./beep"

module T
  @@fails = 0

  def self.check(cond, msg)
    return if cond
    @@fails += 1
    puts "FAIL: #{msg}"
  end

  def self.fails
    @@fails
  end
end

r = Beep.call("POST", "/echo", HTTP::Headers{"Content-Type" => "text/plain", "X-Test" => "crystal"}, "hello from crystal")
T.check(r.status_code == 200 && r.body == "hello from crystal" && r.headers["X-Test"]? == "crystal", "echo #{r.status_code} #{r.body}")
r = Beep.call("GET", "/stream/3")
T.check(r.body == "line 0\nline 1\nline 2\n", "stream")
begin
  Beep.call("GET", "/err/537")
  T.check(false, "expected Beep::Error")
rescue e : Beep::Error
  T.check(e.code == 537 && e.status == 403, "err #{e.code} #{e.status}")
end
T.check(Beep.call("GET", "/status/404").status_code == 404, "status")
puts T.fails == 0 ? "Crystal (gateway): all tests passed" : "Crystal (gateway): #{T.fails} FAILURE(S)"
exit(T.fails == 0 ? 0 : 1)
