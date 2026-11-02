/* libbeep C test: client against the echo server, plus an in-process C server.
 *
 *   gcc -I ../../core/ffi/include test_beep.c <path-to>/beep.dll -o test_beep     (Windows / MinGW)
 *   cc  -I ../../core/ffi/include test_beep.c -L<dir> -lbeep -o test_beep        (Linux / macOS)
 *   ./test_beep 127.0.0.1:10288
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "beep.h"

static int failures = 0;
#define CHECK(cond, ...) do { if (!(cond)) { failures++; printf("FAIL %s:%d: ", __FILE__, __LINE__); printf(__VA_ARGS__); printf("\n"); } } while (0)

/* A C backend served natively over BEEP. */
static void handler(void *user, const beep_request *req, beep_reply *reply) {
    size_t len = 0;
    const uint8_t *body = beep_request_body(req, &len);
    const char *target = beep_request_target(req);
    (void)user;
    if (strcmp(target, "/fail") == 0) {
        beep_reply_error(reply, 550, "c handler refused");
        return;
    }
    beep_reply_status(reply, 202);
    beep_reply_header(reply, "Content-Type", "text/plain");
    beep_reply_header(reply, "X-Served-By", "c");
    {
        char buf[512];
        int n = snprintf(buf, sizeof buf, "%s %s len=%u body=%.*s", beep_request_method(req), target,
                         (unsigned)len, (int)len, (const char *)body);
        beep_reply_body(reply, (const uint8_t *)buf, (size_t)n);
    }
}

int main(int argc, char **argv) {
    const char *addr = argc > 1 ? argv[1] : "127.0.0.1:10288";
    printf("%s\n", beep_version());

    beep_client *c = beep_client_new(addr, 1, 8, 5000);
    CHECK(c != NULL, "client_new: %s", beep_last_error());

    /* 1. echo */
    beep_response *r = NULL;
    const char *msg = "hello from C";
    int rc = beep_call(c, "POST", "/echo?lang=c", "Content-Type: text/plain\nX-Test: c-test\n",
                       (const uint8_t *)msg, strlen(msg), &r);
    CHECK(rc == BEEP_OK, "call rc=%d err=%s", rc, beep_last_error());
    if (rc == BEEP_OK) {
        size_t n = 0;
        const uint8_t *b = beep_response_body(r, &n);
        CHECK(beep_response_status(r) == 200, "status %d", beep_response_status(r));
        CHECK(n == strlen(msg) && memcmp(b, msg, n) == 0, "body mismatch: %.*s", (int)n, b);
        CHECK(beep_response_header(r, "x-test") && strcmp(beep_response_header(r, "X-Test"), "c-test") == 0, "X-Test header");
        CHECK(strstr(beep_response_headers(r), "X-Echo-Method: POST") != NULL, "headers block: %s", beep_response_headers(r));
        beep_response_free(r);
    }

    /* 2. streamed (ANS) response is reassembled */
    rc = beep_call(c, "GET", "/stream/5", NULL, NULL, 0, &r);
    CHECK(rc == BEEP_OK, "stream rc=%d %s", rc, beep_last_error());
    if (rc == BEEP_OK) {
        CHECK(strcmp((const char *)beep_response_body(r, NULL), "line 0\nline 1\nline 2\nline 3\nline 4\n") == 0, "stream body");
        beep_response_free(r);
    }

    /* 3. BEEP ERR */
    rc = beep_call(c, "GET", "/err/537", NULL, NULL, 0, &r);
    CHECK(rc == BEEP_E_REMOTE && beep_last_error_code() == 537, "err rc=%d code=%d", rc, beep_last_error_code());

    /* 4. simple API + binary 1 MB */
    {
        uint8_t *out = NULL;
        size_t out_len = 0;
        int status = beep_call_simple(c, "GET", "/bytes/1048576", NULL, NULL, 0, &out, &out_len);
        CHECK(status == 200 && out_len == 1048576 && out[0] == 'a' && out[26] == 'a', "simple status=%d len=%u", status, (unsigned)out_len);
        beep_free(out);
    }

    /* 5. native C server, called through libbeep itself */
    {
        beep_server *srv = beep_server_start("127.0.0.1:0", handler, NULL);
        CHECK(srv != NULL, "server_start: %s", beep_last_error());
        if (srv) {
            char a[64];
            snprintf(a, sizeof a, "127.0.0.1:%d", beep_server_port(srv));
            beep_client *c2 = beep_client_new(a, 1, 4, 5000);
            rc = beep_call(c2, "PUT", "/c/backend", "X-A: 1", (const uint8_t *)"xyz", 3, &r);
            CHECK(rc == BEEP_OK, "c server rc=%d %s", rc, beep_last_error());
            if (rc == BEEP_OK) {
                CHECK(beep_response_status(r) == 202, "c server status %d", beep_response_status(r));
                CHECK(strcmp((const char *)beep_response_body(r, NULL), "PUT /c/backend len=3 body=xyz") == 0, "c server body: %s", beep_response_body(r, NULL));
                CHECK(strcmp(beep_response_header(r, "x-served-by"), "c") == 0, "served-by");
                beep_response_free(r);
            }
            rc = beep_call(c2, "GET", "/fail", NULL, NULL, 0, &r);
            CHECK(rc == BEEP_E_REMOTE && beep_last_error_code() == 550, "c server fail rc=%d code=%d", rc, beep_last_error_code());
            beep_client_free(c2);
            beep_server_stop(srv);
        }
    }

    /* 6. unreachable peer */
    {
        beep_client *bad = beep_client_new("127.0.0.1:1", 1, 1, 2000);
        rc = beep_call(bad, "GET", "/", NULL, NULL, 0, &r);
        CHECK(rc == BEEP_E_CONNECT || rc == BEEP_E_TIMEOUT, "unreachable rc=%d", rc);
        beep_client_free(bad);
    }

    beep_client_free(c);
    printf(failures ? "C: %d FAILURE(S)\n" : "C: all tests passed\n", failures);
    return failures ? 1 : 0;
}
