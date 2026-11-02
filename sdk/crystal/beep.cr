# beep.cr — Crystal client for BEEP backends via the beepgw HTTP gateway (stdlib HTTP::Client).
# NOT verified locally (no Crystal here).
#   r = Beep.call("POST", "/orders", HTTP::Headers{"Content-Type" => "application/json"}, %({"id":7}))
require "http/client"
require "json"

module Beep
  class Error < Exception
    getter code : Int32
    getter status : Int32

    def initialize(@code, @status, message)
      super(message)
    end
  end

  def self.gateway : String
    ENV.fetch("BEEP_GATEWAY", "http://127.0.0.1:8080")
  end

  def self.call(method : String, target : String, headers = HTTP::Headers.new, body : String? = nil) : HTTP::Client::Response
    res = HTTP::Client.exec(method.upcase, gateway + target, headers: headers, body: body)
    if code = res.headers["X-Beep-Error-Code"]?
      msg = JSON.parse(res.body)["error"]["message"].as_s rescue res.body
      raise Error.new(code.to_i, res.status_code, msg)
    end
    res
  end
end
