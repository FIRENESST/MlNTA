(* ocamlfind ocamlopt -thread -package cohttp-lwt-unix -linkpkg beep.ml test.ml -o test_beep && ./test_beep
   (BEEP_GATEWAY env var) *)
open Lwt.Syntax

let fails = ref 0
let check c msg = if not c then (incr fails; print_endline ("FAIL: " ^ msg))

let () =
  Lwt_main.run
    (let* st, h, body = Beep.call `POST "/echo" ~headers:[ ("Content-Type", "text/plain"); ("X-Test", "ocaml") ] ~body:"hello from ocaml" in
     check (st = 200 && body = "hello from ocaml" && Cohttp.Header.get h "x-test" = Some "ocaml") "echo";
     let* _, _, body = Beep.call `GET "/stream/3" in
     check (body = "line 0\nline 1\nline 2\n") "stream";
     let* err =
       Lwt.catch
         (fun () ->
           let* _ = Beep.call `GET "/err/537" in
           Lwt.return None)
         (function Beep.Beep_error (code, status, _) -> Lwt.return (Some (code, status)) | e -> Lwt.fail e)
     in
     check (err = Some (537, 403)) "err";
     let* st, _, _ = Beep.call `GET "/status/404" in
     check (st = 404) "status";
     Lwt.return_unit);
  if !fails = 0 then print_endline "OCaml (gateway): all tests passed"
  else Printf.printf "OCaml (gateway): %d FAILURE(S)\n" !fails;
  exit (if !fails = 0 then 0 else 1)
