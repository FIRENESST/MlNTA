// beep.dart — Dart / Flutter (dart:ffi) binding to libbeep. Test: test_beep.dart.
// Requires package:ffi.
//   final c = BeepClient('127.0.0.1:10288');
//   final r = c.call('POST', '/echo', headers: {'Content-Type': 'text/plain'}, body: utf8.encode('hi'));
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';

final DynamicLibrary _lib = DynamicLibrary.open(Platform.environment['BEEP_LIB'] ??
    (Platform.isWindows ? 'beep.dll' : Platform.isMacOS ? 'libbeep.dylib' : 'libbeep.so'));

final _new = _lib.lookupFunction<Pointer<Void> Function(Pointer<Utf8>, Int32, Int32, Int32),
    Pointer<Void> Function(Pointer<Utf8>, int, int, int)>('beep_client_new');
final _free = _lib.lookupFunction<Void Function(Pointer<Void>), void Function(Pointer<Void>)>('beep_client_free');
final _call = _lib.lookupFunction<
    Int32 Function(Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Uint8>, Size, Pointer<Pointer<Void>>),
    int Function(Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Uint8>, int, Pointer<Pointer<Void>>)>('beep_call');
final _status = _lib.lookupFunction<Int32 Function(Pointer<Void>), int Function(Pointer<Void>)>('beep_response_status');
final _body = _lib.lookupFunction<Pointer<Uint8> Function(Pointer<Void>, Pointer<Size>),
    Pointer<Uint8> Function(Pointer<Void>, Pointer<Size>)>('beep_response_body');
final _headers = _lib.lookupFunction<Pointer<Utf8> Function(Pointer<Void>), Pointer<Utf8> Function(Pointer<Void>)>('beep_response_headers');
final _respFree = _lib.lookupFunction<Void Function(Pointer<Void>), void Function(Pointer<Void>)>('beep_response_free');
final _lastError = _lib.lookupFunction<Pointer<Utf8> Function(), Pointer<Utf8> Function()>('beep_last_error');
final _lastCode = _lib.lookupFunction<Int32 Function(), int Function()>('beep_last_error_code');

class BeepException implements Exception {
  final int rc, code;
  final String message;
  BeepException(this.rc, this.code, this.message);
  @override
  String toString() => 'BeepException($rc, $code): $message';
}

class BeepResponse {
  final int status;
  final String headers;
  final Uint8List body;
  BeepResponse(this.status, this.headers, this.body);
  String get text => utf8.decode(body, allowMalformed: true);
}

class BeepClient {
  final Pointer<Void> _h;
  BeepClient(String address, {int timeoutMs = 30000})
      : _h = using((a) => _new(address.toNativeUtf8(allocator: a), 1, 16, timeoutMs));

  BeepResponse call(String method, String target, {Map<String, String> headers = const {}, List<int> body = const []}) {
    return using((a) {
      final buf = a<Uint8>(body.isEmpty ? 1 : body.length);
      buf.asTypedList(body.length).setAll(0, body);
      final out = a<Pointer<Void>>();
      final hs = headers.entries.map((e) => '${e.key}: ${e.value}').join('\n');
      final rc = _call(_h, method.toNativeUtf8(allocator: a), target.toNativeUtf8(allocator: a),
          hs.toNativeUtf8(allocator: a), buf, body.length, out);
      if (rc != 0) throw BeepException(rc, _lastCode(), _lastError().toDartString());
      final n = a<Size>();
      final p = _body(out.value, n);
      final r = BeepResponse(_status(out.value), _headers(out.value).toDartString(), Uint8List.fromList(p.asTypedList(n.value)));
      _respFree(out.value);
      return r;
    });
  }

  void close() => _free(_h);
}
