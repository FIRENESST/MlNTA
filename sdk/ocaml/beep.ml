(* beep.ml — OCaml client for BEEP backends via the beepgw HTTP gateway (cohttp-lwt-unix).
   NOT verified locally (no OCaml here).
     Lwt_main.run (Beep.call `POST "/orders" ~headers:["Content-Type", "application/json"] ~body:{|{"id":7}|}) *)
open Lwt.Syntax

exception Beep_error of int * int * string

let gateway () = Option.value (Sys.getenv_opt "BEEP_GATEWAY") ~default:"http://127.0.0.1:8080"

let call ?(headers = []) ?(body = "") meth target =
  let uri = Uri.of_string (gateway () ^ target) in
  let headers = Cohttp.Header.of_list headers in
  let* resp, rbody = Cohttp_lwt_unix.Client.call ~headers ~body:(Cohttp_lwt.Body.of_string body) meth uri in
  let* text = Cohttp_lwt.Body.to_string rbody in
  let status = Cohttp.Code.code_of_status (Cohttp.Response.status resp) in
  match Cohttp.Header.get (Cohttp.Response.headers resp) "x-beep-error-code" with
  | Some code -> Lwt.fail (Beep_error (int_of_string code, status, text))
  | None -> Lwt.return (status, Cohttp.Response.headers resp, text)
