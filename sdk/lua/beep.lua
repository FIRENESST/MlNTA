-- beep.lua — Lua 5.1–5.4 client for BEEP backends via the beepgw HTTP gateway (LuaSocket).
-- Test: test.lua. For LuaJIT native BEEP see bindings/luajit.
--   local beep = require "beep"
--   local status, body, headers = beep.call("POST", "/orders", {["Content-Type"]="application/json"}, '{"id":7}')
local http = require "socket.http"
local ltn12 = require "ltn12"

local M = { gateway = os.getenv("BEEP_GATEWAY") or "http://127.0.0.1:8080" }

--- Returns status, body, headers — or nil, message, beep_code for a BEEP ERR.
function M.call(method, target, headers, body)
  local out = {}
  local h = {}
  for k, v in pairs(headers or {}) do h[k] = v end
  if body then h["Content-Length"] = tostring(#body) end
  local ok, status, rh = http.request {
    url = M.gateway .. target,
    method = method:upper(),
    headers = h,
    source = body and ltn12.source.string(body) or nil,
    sink = ltn12.sink.table(out),
  }
  if not ok then return nil, status end
  local data = table.concat(out)
  local code = rh and rh["x-beep-error-code"]
  if code then
    return nil, data:match('"message"%s*:%s*"([^"]*)"') or data, tonumber(code)
  end
  return status, data, rh
end

return M
