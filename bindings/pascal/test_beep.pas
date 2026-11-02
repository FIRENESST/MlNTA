{ fpc -Fu. -Fl/path/to/release test_beep.pas && ./test_beep 127.0.0.1:10288 }
program test_beep;

{$IFDEF FPC}{$MODE DELPHI}{$ENDIF}

uses beep;

var
  c: PBeepClient;
  status, failures, i, j: Integer;
  address, body, bin: AnsiString;

procedure Check(cond: Boolean; const msg: string);
begin
  if not cond then
  begin
    Inc(failures);
    WriteLn('FAIL: ', msg);
  end;
end;

begin
  failures := 0;
  if ParamCount > 0 then address := ParamStr(1) else address := '127.0.0.1:10288';
  c := beep_client_new(PAnsiChar(address), 1, 4, 5000);
  Check(c <> nil, 'client');
  status := BeepCallSimple(c, 'POST', '/echo', 'Content-Type: text/plain', 'hello from pascal', body);
  Check((status = 200) and (body = 'hello from pascal'), 'echo');
  BeepCallSimple(c, 'GET', '/stream/3', '', '', body);
  Check(body = 'line 0'#10'line 1'#10'line 2'#10, 'stream');
  status := BeepCallSimple(c, 'GET', '/err/537', '', '', body);
  Check((status = BEEP_E_REMOTE) and (beep_last_error_code = 537), 'err');
  bin := '';
  for i := 1 to 10 do
    for j := 0 to 255 do
      bin := bin + AnsiChar(j);
  BeepCallSimple(c, 'PUT', '/echo', '', bin, body);
  Check(body = bin, 'binary-safe body');
  beep_client_free(c);
  if failures = 0 then
    WriteLn('Pascal (FFI): all tests passed')
  else
    WriteLn('Pascal (FFI): ', failures, ' FAILURE(S)');
  if failures <> 0 then Halt(1);
end.
