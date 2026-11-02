# frozen_string_literal: true

# beep_gateway.rb — Ruby client for BEEP backends via the beepgw HTTP gateway (stdlib Net::HTTP).
# For native BEEP without the gateway use bindings/ruby (Fiddle + libbeep).
#
#   require_relative 'beep_gateway'
#   gw = Beep::Gateway.new('http://127.0.0.1:8080')
#   r = gw.call('POST', '/orders', headers: { 'Content-Type' => 'application/json' }, body: '{"id":7}')
require 'net/http'
require 'json'
require 'uri'

module Beep
  class GatewayError < StandardError
    attr_reader :code, :status

    def initialize(code, status, message)
      @code = code
      @status = status
      super(message)
    end
  end

  class Gateway
    Response = Struct.new(:status, :headers, :body)

    def initialize(base = ENV.fetch('BEEP_GATEWAY', 'http://127.0.0.1:8080'), upstream: nil)
      @uri = URI(base.chomp('/'))
      @upstream = upstream
      @http = Net::HTTP.new(@uri.host, @uri.port)
      @http.read_timeout = 120
      @lock = Mutex.new
    end

    def call(method, target, headers: {}, body: nil, timeout_ms: nil)
      req = Net::HTTPGenericRequest.new(method.to_s.upcase, !body.nil?, true, @uri.path + target)
      headers.each { |k, v| req[k] = v }
      req['X-Beep-Upstream'] = @upstream if @upstream
      req['X-Beep-Timeout-Ms'] = timeout_ms.to_s if timeout_ms
      req.body = body if body
      res = @lock.synchronize do
        @http.start unless @http.started?
        @http.request(req)
      end
      if (code = res['x-beep-error-code'])
        msg = begin
          JSON.parse(res.body).dig('error', 'message')
        rescue StandardError
          res.body
        end
        raise GatewayError.new(code.to_i, res.code.to_i, msg)
      end
      Response.new(res.code.to_i, res.to_hash.transform_values { |v| v.join(', ') }, res.body.to_s)
    end
  end
end
