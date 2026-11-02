-- beep.lua — LuaJIT FFI binding to libbeep (native BEEP). Test: test_beep.lua.
--   local beep = require "beep"
--   local c = beep.client("127.0.0.1:10288")
--   local status, body, headers = c:call("POST", "/echo", {["Content-Type"]="text/plain"}, "hi")
-- Plain Lua 5.x without FFI: use sdk/lua (LuaSocket + gateway).
local ffi = require "ffi"
ffi.cdef [[
typedef struct beep_client beep_client;
typedef struct beep_response beep_response;
beep_client *beep_client_new(const char *address, int sessions, int max_channels, int timeout_ms);
void beep_client_free(beep_client *c);
int beep_call(beep_client *c, const char *method, const char *target, const char *headers,
              const uint8_t *body, size_t body_len, beep_response **out);
int beep_response_status(const beep_response *r);
const uint8_t *beep_response_body(const beep_response *r, size_t *len);
const char *beep_response_headers(const beep_response *r);
void beep_response_free(beep_response *r);
const char *beep_last_error(void);
int beep_last_error_code(void);
]]
local lib = ffi.load(os.getenv("BEEP_LIB") or "beep")
local M = {}
local Client = {}
Client.__index = Client

function M.client(address, timeout_ms)
  local h = lib.beep_client_new(address, 1, 16, timeout_ms or 30000)
  assert(h ~= nil, ffi.string(lib.beep_last_error()))
  return setmetatable({ h = ffi.gc(h, lib.beep_client_free) }, Client)
end

--- Returns status, body, headers — or nil, message, beep_code on failure.
function Client:call(method, target, headers, body)
  local lines = {}
  for k, v in pairs(headers or {}) do lines[#lines + 1] = k .. ": " .. v end
  body = body or ""
  local out = ffi.new("beep_response*[1]")
  local rc = lib.beep_call(self.h, method, target, table.concat(lines, "\n"), body, #body, out)
  if rc ~= 0 then
    return nil, ffi.string(lib.beep_last_error()), lib.beep_last_error_code()
  end
  local r = out[0]
  local n = ffi.new("size_t[1]")
  local ptr = lib.beep_response_body(r, n)
  local status, data = lib.beep_response_status(r), ffi.string(ptr, n[0])
  local hdrs = {}
  for k, v in ffi.string(lib.beep_response_headers(r)):gmatch("([^:\r\n]+):%s*([^\r\n]*)\r\n") do hdrs[k:lower()] = v end
  lib.beep_response_free(r)
  return status, data, hdrs
end

return M
