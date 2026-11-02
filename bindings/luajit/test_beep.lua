-- luajit test_beep.lua 127.0.0.1:10288   (BEEP_LIB=/path/to/libbeep.so)
package.path = (arg[0]:match("^(.*[/\\])") or "./") .. "?.lua;" .. package.path
local beep = require "beep"

local failures = 0
local function check(cond, msg)
  if cond then return end
  failures = failures + 1
  print("FAIL: " .. msg)
end

local c = beep.client(arg[1] or "127.0.0.1:10288", 5000)
local status, body, h = c:call("POST", "/echo", { ["Content-Type"] = "text/plain", ["X-Test"] = "luajit" }, "hello from luajit")
check(status == 200 and body == "hello from luajit" and h["x-test"] == "luajit", "echo " .. tostring(status) .. " " .. tostring(body))
status, body = c:call("GET", "/stream/3")
check(body == "line 0\nline 1\nline 2\n", "stream " .. tostring(body))
local s, msg, code = c:call("GET", "/err/537")
check(s == nil and code == 537, "err " .. tostring(msg) .. " " .. tostring(code))
local bytes = {}
for i = 0, 255 do bytes[#bytes + 1] = string.char(i) end
local bin = string.rep(table.concat(bytes), 10)
status, body = c:call("PUT", "/echo", nil, bin)
check(body == bin, "binary-safe body")
print(failures == 0 and "LuaJIT (FFI): all tests passed" or ("LuaJIT (FFI): " .. failures .. " FAILURE(S)"))
os.exit(failures == 0 and 0 or 1)
