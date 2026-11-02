# beep.R — R client for BEEP backends via the beepgw HTTP gateway (package `curl`).
# Test: test.R.
#   source("beep.R")
#   r <- beep_call("POST", "/orders", headers = c("Content-Type" = "application/json"), body = '{"id":7}')
#   r$status; rawToChar(r$body)
library(curl)

beep_gateway <- function() Sys.getenv("BEEP_GATEWAY", "http://127.0.0.1:8080")

beep_call <- function(method, target, headers = character(), body = NULL, upstream = NULL, timeout_ms = NULL) {
  h <- new_handle(customrequest = toupper(method))
  hdrs <- headers
  if (!is.null(upstream)) hdrs["X-Beep-Upstream"] <- upstream
  if (!is.null(timeout_ms)) hdrs["X-Beep-Timeout-Ms"] <- as.character(timeout_ms)
  if (length(hdrs)) handle_setheaders(h, .list = as.list(hdrs))
  if (!is.null(body)) {
    raw <- if (is.raw(body)) body else charToRaw(enc2utf8(as.character(body)))
    handle_setopt(h, postfields = raw)
  }
  res <- curl_fetch_memory(paste0(beep_gateway(), target), handle = h)
  parsed <- parse_headers_list(res$headers)
  code <- parsed[["x-beep-error-code"]]
  if (!is.null(code)) {
    msg <- tryCatch(jsonlite::fromJSON(rawToChar(res$content))$error$message, error = function(e) rawToChar(res$content))
    stop(structure(class = c("beep_error", "error", "condition"),
                   list(message = sprintf("beep error %s: %s", code, msg), call = NULL, code = as.integer(code), status = res$status_code)))
  }
  list(status = res$status_code, headers = parsed, body = res$content)
}
