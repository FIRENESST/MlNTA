{ beep.pas - Free Pascal / Delphi binding to libbeep. NOT verified locally (no FPC/Delphi here).
    uses beep;
    var c: PBeepClient; status: Integer; body: AnsiString;
    c := beep_client_new('127.0.0.1:10288', 1, 16, 30000);
    status := BeepCallSimple(c, 'POST', '/echo', 'Content-Type: text/plain', 'hi', body); }
unit beep;

{$IFDEF FPC}{$MODE DELPHI}{$ENDIF}

interface

const
{$IFDEF MSWINDOWS}
  BeepLib = 'beep.dll';
{$ELSE}
  {$IFDEF DARWIN}
  BeepLib = 'libbeep.dylib';
  {$ELSE}
  BeepLib = 'libbeep.so';
  {$ENDIF}
{$ENDIF}
  BEEP_OK = 0;
  BEEP_E_REMOTE = -3;

type
  PBeepClient = Pointer;
  PPByte = ^PByte;
  PNativeUInt = ^NativeUInt;

function beep_client_new(address: PAnsiChar; sessions, maxChannels, timeoutMs: Integer): PBeepClient; cdecl; external BeepLib;
procedure beep_client_free(c: PBeepClient); cdecl; external BeepLib;
function beep_call_simple(c: PBeepClient; method, target, headers: PAnsiChar; body: PByte; bodyLen: NativeUInt;
  outBody: PPByte; outLen: PNativeUInt): Integer; cdecl; external BeepLib;
procedure beep_free(p: Pointer); cdecl; external BeepLib;
function beep_last_error: PAnsiChar; cdecl; external BeepLib;
function beep_last_error_code: Integer; cdecl; external BeepLib;

{ Returns the status (>= 100) or a negative BEEP_E_* code; Body receives the response body. }
function BeepCallSimple(c: PBeepClient; const Method, Target, Headers, Req: AnsiString; out Body: AnsiString): Integer;

implementation

function BeepCallSimple(c: PBeepClient; const Method, Target, Headers, Req: AnsiString; out Body: AnsiString): Integer;
var
  p: PByte;
  n: NativeUInt;
begin
  p := nil;
  n := 0;
  Result := beep_call_simple(c, PAnsiChar(Method), PAnsiChar(Target), PAnsiChar(Headers),
    PByte(PAnsiChar(Req)), Length(Req), @p, @n);
  SetLength(Body, n);
  if (p <> nil) and (n > 0) then Move(p^, Body[1], n);
  if p <> nil then beep_free(p);
end;

end.
