#!/usr/bin/env sh
# beep.sh — call BEEP backends from any POSIX shell through the beepgw HTTP gateway (needs curl).
#
#   . ./beep.sh                       # source for the beep_call function, or run directly:
#   ./beep.sh POST /orders -H 'Content-Type: application/json' -d '{"id":7}'
#
# Environment:
#   BEEP_GATEWAY     gateway base URL          (default http://127.0.0.1:8080)
#   BEEP_UPSTREAM    named upstream to select  (X-Beep-Upstream)
#   BEEP_TIMEOUT_MS  per-request deadline      (X-Beep-Timeout-Ms)
#
# Exit status: 0 on 2xx/3xx, 22 on HTTP errors (curl --fail-with-body semantics), curl's code otherwise.
# On a BEEP ERR the gateway answers with X-Beep-Error-Code and a JSON body {"error":{"code","message"}}.

beep_call() {
    _m="$1"; _t="$2"; shift 2
    set -- -sS --fail-with-body -X "$_m" "$@"
    [ -n "$BEEP_UPSTREAM" ] && set -- "$@" -H "X-Beep-Upstream: $BEEP_UPSTREAM"
    [ -n "$BEEP_TIMEOUT_MS" ] && set -- "$@" -H "X-Beep-Timeout-Ms: $BEEP_TIMEOUT_MS"
    curl "$@" "${BEEP_GATEWAY:-http://127.0.0.1:8080}$_t"
}

# Raw passthrough to any BEEP profile: beep_raw PROFILE_URI < payload-entity
beep_raw() {
    curl -sS --fail-with-body -X POST -H "X-Beep-Profile: $1" --data-binary @- "${BEEP_GATEWAY:-http://127.0.0.1:8080}/__beep/raw"
}

beep_health() {
    curl -sS "${BEEP_GATEWAY:-http://127.0.0.1:8080}/__beep/health"
}

case "$0" in
    *beep.sh)
        if [ $# -lt 2 ]; then
            echo "usage: $0 METHOD TARGET [curl options...]" >&2
            exit 2
        fi
        beep_call "$@"
        ;;
esac
