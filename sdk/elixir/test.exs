# elixir test.exs   (BEEP_GATEWAY env var)
Code.require_file("beep.ex", __DIR__)

check = fn
  true, _msg -> 0
  _, msg -> IO.puts("FAIL: #{msg}"); 1
end

{:ok, r1} = Beep.call(:post, "/echo", [{"Content-Type", "text/plain"}, {"X-Test", "elixir"}], "hello from elixir")
f1 = check.(r1.status == 200 and r1.body == "hello from elixir" and List.keyfind(r1.headers, "x-test", 0) == {"x-test", "elixir"}, "echo #{inspect(r1)}")
{:ok, r2} = Beep.call(:get, "/stream/3")
f2 = check.(r2.body == "line 0\nline 1\nline 2\n", "stream")
f3 =
  case Beep.call(:get, "/err/537") do
    {:error, {:beep, 537, 403, _}} -> 0
    other -> check.(false, "err #{inspect(other)}")
  end
{:ok, r4} = Beep.call(:get, "/status/404")
f4 = check.(r4.status == 404, "status")

fails = f1 + f2 + f3 + f4
IO.puts(if fails == 0, do: "Elixir (gateway): all tests passed", else: "Elixir (gateway): #{fails} FAILURE(S)")
System.halt(if fails == 0, do: 0, else: 1)
