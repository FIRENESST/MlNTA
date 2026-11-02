// dart pub get && dart run test_beep.dart 127.0.0.1:10288   (BEEP_LIB=/path/to/libbeep.so)
import 'dart:convert';
import 'dart:io';

import 'beep.dart';

var failures = 0;

void check(bool cond, String msg) {
  if (cond) return;
  failures++;
  print('FAIL: $msg');
}

void main(List<String> args) {
  final c = BeepClient(args.isNotEmpty ? args[0] : '127.0.0.1:10288', timeoutMs: 5000);
  var r = c.call('POST', '/echo', headers: {'Content-Type': 'text/plain', 'X-Test': 'dart'}, body: utf8.encode('hello from dart'));
  check(r.status == 200 && r.text == 'hello from dart' && r.headers.toLowerCase().contains('x-test: dart'), 'echo ${r.status} ${r.text}');
  r = c.call('GET', '/stream/3');
  check(r.text == 'line 0\nline 1\nline 2\n', 'stream');
  try {
    c.call('GET', '/err/537');
    check(false, 'expected BeepException');
  } on BeepException catch (e) {
    check(e.rc == -3 && e.code == 537, 'err $e');
  }
  final bin = [for (var k = 0; k < 10; k++) ...List.generate(256, (i) => i)];
  r = c.call('PUT', '/echo', body: bin);
  var same = r.body.length == bin.length;
  for (var i = 0; same && i < bin.length; i++) {
    same = r.body[i] == bin[i];
  }
  check(same, 'binary-safe body');
  c.close();
  print(failures == 0 ? 'Dart (FFI): all tests passed' : 'Dart (FFI): $failures FAILURE(S)');
  exit(failures == 0 ? 0 : 1);
}
