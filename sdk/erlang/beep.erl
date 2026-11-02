%% beep.erl — Erlang/OTP client for BEEP backends via the beepgw HTTP gateway (inets/httpc).
%% Test: beep_test.erl.
%%   {ok, #{status := 200, body := B}} = beep:call(post, "/orders", [{"content-type", "application/json"}], <<"{\"id\":7}">>).
-module(beep).
-export([call/2, call/4, gateway/0]).

gateway() -> os:getenv("BEEP_GATEWAY", "http://127.0.0.1:8080").

call(Method, Target) -> call(Method, Target, [], undefined).

call(Method, Target, Headers, Body) ->
    _ = application:ensure_all_started(inets),
    Url = gateway() ++ Target,
    {CType, Rest} = case lists:partition(fun({K, _}) -> string:lowercase(K) =:= "content-type" end, Headers) of
                        {[{_, C} | _], R} -> {C, R};
                        {[], R} -> {"application/octet-stream", R}
                    end,
    Req = case Body of
              undefined -> {Url, Rest};
              _ -> {Url, Rest, CType, Body}
          end,
    case httpc:request(Method, Req, [{timeout, 120000}], [{body_format, binary}]) of
        {ok, {{_, Status, _}, RespHeaders, RespBody}} ->
            case lists:keyfind("x-beep-error-code", 1, RespHeaders) of
                {_, Code} -> {error, {beep, list_to_integer(Code), Status, RespBody}};
                false -> {ok, #{status => Status, headers => RespHeaders, body => RespBody}}
            end;
        {error, Reason} -> {error, Reason}
    end.
