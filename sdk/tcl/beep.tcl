# beep.tcl — Tcl 8.6+ client for BEEP backends via the beepgw HTTP gateway.
#
#   source beep.tcl
#   set r [beep::call POST /orders -headers {Content-Type application/json} -body {{"id":7}}]
#   puts "[dict get $r status] [dict get $r body]"
#
# On a BEEP ERR it raises an error with -errorcode {BEEP <code> <http-status>}.
package require http
package provide beep 0.1

namespace eval beep {
    variable gateway [expr {[info exists ::env(BEEP_GATEWAY)] ? $::env(BEEP_GATEWAY) : "http://127.0.0.1:8080"}]

    proc configure {url} {
        variable gateway
        set gateway [string trimright $url /]
    }

    proc call {method target args} {
        variable gateway
        set opts [dict merge {-headers {} -body "" -upstream "" -timeout_ms 0} $args]
        set hdrs [dict get $opts -headers]
        if {[dict get $opts -upstream] ne ""} { lappend hdrs X-Beep-Upstream [dict get $opts -upstream] }
        if {[dict get $opts -timeout_ms] > 0} { lappend hdrs X-Beep-Timeout-Ms [dict get $opts -timeout_ms] }
        set ctype application/octet-stream
        foreach {k v} $hdrs { if {[string equal -nocase $k Content-Type]} { set ctype $v } }
        set cmd [list ::http::geturl $gateway$target -method [string toupper $method] -headers $hdrs -binary 1]
        if {[dict get $opts -body] ne "" || $method in {POST PUT PATCH}} {
            lappend cmd -query [dict get $opts -body] -type $ctype
        }
        set tok [{*}$cmd]
        try {
            if {[::http::status $tok] ne "ok"} { error "beep: [::http::status $tok] [::http::error $tok]" }
            set meta {}
            foreach {k v} [::http::meta $tok] { dict set meta [string tolower $k] $v }
            set r [dict create status [::http::ncode $tok] headers $meta body [::http::data $tok]]
        } finally {
            ::http::cleanup $tok
        }
        if {[dict exists $meta x-beep-error-code]} {
            set code [dict get $meta x-beep-error-code]
            set msg [dict get $r body]
            regexp {"message"\s*:\s*"([^"]*)"} $msg -> msg
            return -code error -errorcode [list BEEP $code [dict get $r status]] "beep error $code: $msg"
        }
        return $r
    }
}
