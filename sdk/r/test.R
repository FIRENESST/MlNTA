# Rscript test.R   (BEEP_GATEWAY env var)
here <- dirname(sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE)[1]))
source(file.path(here, "beep.R"))

fails <- 0
check <- function(cond, msg) {
  if (!isTRUE(cond)) {
    fails <<- fails + 1
    cat("FAIL:", msg, "\n")
  }
}

r <- beep_call("POST", "/echo", headers = c("Content-Type" = "text/plain", "X-Test" = "r"), body = "hello from r")
check(r$status == 200 && rawToChar(r$body) == "hello from r" && identical(r$headers[["x-test"]], "r"), "echo")
r <- beep_call("GET", "/stream/3")
check(rawToChar(r$body) == "line 0\nline 1\nline 2\n", "stream")
e <- tryCatch({
  beep_call("GET", "/err/537")
  NULL
}, beep_error = function(e) e)
check(!is.null(e) && e$code == 537 && e$status == 403, "err")
check(beep_call("GET", "/status/404")$status == 404, "status")
cat(if (fails == 0) "R (gateway): all tests passed\n" else sprintf("R (gateway): %d FAILURE(S)\n", fails))
quit(status = if (fails == 0) 0 else 1)
