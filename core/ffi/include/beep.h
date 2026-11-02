/*
 * libbeep — C ABI of the MINTA BEEP bridge.
 *
 * Native RFC 3080/3081 BEEP + the RPC profile "urn:minta:beep:rpc:1" for any language
 * that can call C: C, C++, Go (cgo), Python (ctypes), Ruby (Fiddle/FFI), PHP (FFI),
 * Lua (LuaJIT FFI), Zig, Nim, D, Swift, Dart (dart:ffi), Julia (ccall), Fortran
 * (ISO_C_BINDING), Delphi/Free Pascal, Haskell, OCaml, Ada, Crystal, V, Odin, ...
 *
 * Threading: every function is thread-safe. Client calls block the calling thread.
 * Server handlers run on a worker pool and may be invoked concurrently.
 *
 * Memory: objects returned by *_new / beep_call / beep_server_start are owned by the
 * caller and released with the matching *_free / *_stop. Pointers returned by accessors
 * stay valid until the owning object is freed. Buffers from beep_call_simple are released
 * with beep_free.
 */
#ifndef MINTA_BEEP_H
#define MINTA_BEEP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32) && !defined(BEEP_STATIC)
#  define BEEP_API __declspec(dllimport)
#else
#  define BEEP_API
#endif

/* Return codes (negative = failure; details via beep_last_error*). */
#define BEEP_OK              0
#define BEEP_E_ARG          -1   /* invalid argument */
#define BEEP_E_CONNECT      -2   /* could not reach the BEEP peer */
#define BEEP_E_REMOTE       -3   /* peer answered BEEP ERR; code in beep_last_error_code() */
#define BEEP_E_TIMEOUT      -4
#define BEEP_E_TRANSPORT    -5   /* session failed mid-request */
#define BEEP_E_INTERNAL     -6

typedef struct beep_client   beep_client;
typedef struct beep_response beep_response;
typedef struct beep_server   beep_server;
typedef struct beep_request  beep_request;
typedef struct beep_reply    beep_reply;

/* ---- diagnostics ------------------------------------------------------- */

BEEP_API const char *beep_version(void);
/* Message for the last failure on the calling thread ("" if none). */
BEEP_API const char *beep_last_error(void);
/* BEEP reply code (RFC 3080 §8, e.g. 550) for the last failure on this thread, 0 if none. */
BEEP_API int beep_last_error_code(void);

/* ---- client ------------------------------------------------------------ */

/* address: "host:port". sessions/max_channels <= 0 pick defaults (1 / 64).
 * timeout_ms <= 0 disables the per-request deadline. Returns NULL on bad arguments.
 * Connection is lazy: the first call connects. */
BEEP_API beep_client *beep_client_new(const char *address, int sessions, int max_channels, int timeout_ms);
/* Same, negotiating the BEEP TLS profile. ca_file may be NULL (system/Mozilla roots). */
BEEP_API beep_client *beep_client_new_tls(const char *address, int sessions, int max_channels, int timeout_ms,
                                          const char *server_name, const char *ca_file);
BEEP_API void beep_client_free(beep_client *client);

/* Perform one request. headers: "Name: value" lines separated by "\n" or "\r\n" (may be NULL).
 * On BEEP_OK, *out receives a response to release with beep_response_free. */
BEEP_API int beep_call(beep_client *client, const char *method, const char *target, const char *headers,
                       const uint8_t *body, size_t body_len, beep_response **out);

BEEP_API int beep_response_status(const beep_response *resp);
/* Body bytes; *len receives the length. The buffer is additionally NUL-terminated. */
BEEP_API const uint8_t *beep_response_body(const beep_response *resp, size_t *len);
/* All headers as "Name: value\r\n" lines, NUL-terminated. */
BEEP_API const char *beep_response_headers(const beep_response *resp);
/* First header with this name (case-insensitive), or NULL. */
BEEP_API const char *beep_response_header(const beep_response *resp, const char *name);
BEEP_API void beep_response_free(beep_response *resp);

/* One-shot helper for FFIs that dislike opaque handles.
 * Returns the HTTP-style status (>= 100) or a negative BEEP_E_* code.
 * *out_body (may be NULL) receives a NUL-terminated copy of the body, free with beep_free. */
BEEP_API int beep_call_simple(beep_client *client, const char *method, const char *target, const char *headers,
                              const uint8_t *body, size_t body_len, uint8_t **out_body, size_t *out_len);
BEEP_API void beep_free(void *ptr);

/* ---- server ------------------------------------------------------------ */

/* Called once per request on a worker thread. Fill `reply` before returning; both pointers
 * are only valid during the call. If nothing is set the reply is 200 with an empty body. */
typedef void (*beep_handler)(void *user_data, const beep_request *req, beep_reply *reply);

/* listen: "host:port" (port 0 = ephemeral). Returns NULL on failure (see beep_last_error). */
BEEP_API beep_server *beep_server_start(const char *listen, beep_handler handler, void *user_data);
BEEP_API beep_server *beep_server_start_tls(const char *listen, beep_handler handler, void *user_data,
                                            const char *cert_file, const char *key_file);
BEEP_API int beep_server_port(const beep_server *server);
BEEP_API void beep_server_stop(beep_server *server);

BEEP_API const char *beep_request_method(const beep_request *req);
BEEP_API const char *beep_request_target(const beep_request *req);
BEEP_API const char *beep_request_headers(const beep_request *req);
BEEP_API const char *beep_request_header(const beep_request *req, const char *name);
BEEP_API const uint8_t *beep_request_body(const beep_request *req, size_t *len);

BEEP_API void beep_reply_status(beep_reply *reply, int status);
BEEP_API void beep_reply_header(beep_reply *reply, const char *name, const char *value);
/* The body is copied. */
BEEP_API void beep_reply_body(beep_reply *reply, const uint8_t *body, size_t len);
/* Answer with BEEP ERR (no application response), e.g. code 550. */
BEEP_API void beep_reply_error(beep_reply *reply, int code, const char *message);

#ifdef __cplusplus
}
#endif
#endif /* MINTA_BEEP_H */
