%% erlc beep.erl beep_test.erl && erl -noshell -s beep_test run   (BEEP_GATEWAY env var)
-module(beep_test).
-export([run/0]).

run() ->
    Echo = case beep:call(post, "/echo", [{"content-type", "text/plain"}, {"x-test", "erlang"}], <<"hello from erlang">>) of
               {ok, #{status := 200, body := <<"hello from erlang">>, headers := H}} ->
                   lists:keyfind("x-test", 1, H) =:= {"x-test", "erlang"};
               _ -> false
           end,
    Stream = case beep:call(get, "/stream/3") of
                 {ok, #{body := <<"line 0\nline 1\nline 2\n">>}} -> true;
                 _ -> false
             end,
    Err = case beep:call(get, "/err/537") of
              {error, {beep, 537, 403, _}} -> true;
              _ -> false
          end,
    Status = case beep:call(get, "/status/404") of
                 {ok, #{status := 404}} -> true;
                 _ -> false
             end,
    Failed = [Name || {Name, false} <- [{echo, Echo}, {stream, Stream}, {err, Err}, {status, Status}]],
    [io:format("FAIL: ~p~n", [N]) || N <- Failed],
    case Failed of
        [] -> io:format("Erlang (gateway): all tests passed~n"), halt(0);
        _ -> io:format("Erlang (gateway): ~p FAILURE(S)~n", [length(Failed)]), halt(1)
    end.
