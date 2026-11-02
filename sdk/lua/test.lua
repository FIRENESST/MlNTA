-- lua test.lua   (BEEP_GATEWAY env var; needs LuaSocket)
package.path = (arg[0]:match("^(.*[/\\])") or "./") .. "?.lua;" .. package.path
local beep = require "beep"

local fails = 0
local function check(cond, msg)
  if cond then return end
  fails = fails + 1
  print("FAIL: " .. msg)
end

local status, body, h = beep.call("POST", "/echo", { ["Content-Type"] = "text/plain", ["X-Test"] = "lua" }, "hello from lua")
check(status == 200 and body == "hello from lua" and h["x-test"] == "lua", "echo " .. tostring(status) .. " " .. tostring(body))
status, body = beep.call("GET", "/stream/3")
check(body == "line 0\nline 1\nline 2\n", "stream " .. tostring(body))
local s, msg, code = beep.call("GET", "/err/537")
check(s == nil and code == 537, "err " .. tostring(msg) .. " " .. tostring(code))
check(beep.call("GET", "/status/404") == 404, "status")
print(fails == 0 and "Lua (gateway): all tests passed" or ("Lua (gateway): " .. fails .. " FAILURE(S)"))
os.exit(fails == 0 and 0 or 1)
