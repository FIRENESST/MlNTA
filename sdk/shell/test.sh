#!/usr/bin/env sh
# BEEP_GATEWAY=http://127.0.0.1:8080 sh test.sh
. "$(dirname "$0")/beep.sh"
fail=0
check() { if [ "$1" != "$2" ]; then echo "FAIL: $3: got [$1] want [$2]"; fail=$((fail+1)); fi; }
check "$(beep_call POST /echo -H 'Content-Type: text/plain' -d 'hello from sh')" "hello from sh" echo
check "$(beep_call GET /stream/3 | tr '\n' ',')" "line 0,line 1,line 2," stream
out=$(beep_call GET /err/537 -D - 2>/dev/null); rc=$?
check "$rc" "22" "err exit code"
case "$out" in *"x-beep-error-code: 537"*) ;; *) echo "FAIL: err header missing: $out"; fail=$((fail+1));; esac
check "$(curl -s -o /dev/null -w '%{http_code}' "$BEEP_GATEWAY/status/404")" "404" status
case "$(beep_health)" in *'"status":"ok"'*) ;; *) echo "FAIL: health"; fail=$((fail+1));; esac
[ $fail -eq 0 ] && echo "Shell/curl (gateway): all tests passed" || { echo "Shell: $fail FAILURE(S)"; exit 1; }
