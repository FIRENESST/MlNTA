# frozen_string_literal: true

# beep.rb — Ruby binding to libbeep via Fiddle (stdlib; no gems to compile).
#
#   require_relative 'beep'
#   c = Beep::Client.new('127.0.0.1:10288')
#   r = c.call('POST', '/orders', headers: { 'Content-Type' => 'application/json' }, body: '{"id":7}')
#   puts r.status, r.body
#
# Library lookup: ENV['BEEP_LIB'] or beep.dll / libbeep.so / libbeep.dylib on the loader path.
# Serving Ruby apps (Rack/Rails/Sinatra) over BEEP: run them behind `beepgw expose`.
require 'fiddle'
require 'fiddle/import'

module Beep
  E_ARG = -1
  E_CONNECT = -2
  E_REMOTE = -3
  E_TIMEOUT = -4
  E_TRANSPORT = -5

  module Lib
    extend Fiddle::Importer
    default = if Gem.win_platform? then 'beep.dll'
              elsif RUBY_PLATFORM.include?('darwin') then 'libbeep.dylib'
              else 'libbeep.so'
              end
    dlload ENV.fetch('BEEP_LIB', default)
    extern 'void* beep_client_new(const char*, int, int, int)'
    extern 'void* beep_client_new_tls(const char*, int, int, int, const char*, const char*)'
    extern 'void beep_client_free(void*)'
    extern 'int beep_call(void*, const char*, const char*, const char*, const char*, size_t, void*)'
    extern 'int beep_response_status(void*)'
    extern 'void* beep_response_body(void*, void*)'
    extern 'const char* beep_response_headers(void*)'
    extern 'void beep_response_free(void*)'
    extern 'const char* beep_last_error()'
    extern 'int beep_last_error_code()'
    extern 'const char* beep_version()'
  end

  # Failure of a call: +rc+ is the BEEP_E_* value, +code+ the BEEP reply code (e.g. 550).
  class Error < StandardError
    attr_reader :rc, :code

    def initialize(rc)
      @rc = rc
      @code = Lib.beep_last_error_code
      super(Lib.beep_last_error.to_s)
    end
  end

  Response = Struct.new(:status, :headers, :body) do
    def [](name)
      pair = headers.find { |k, _| k.casecmp?(name) }
      pair && pair[1]
    end
  end

  def self.version = Lib.beep_version.to_s

  # Thread-safe pooled client (native BEEP, no gateway).
  class Client
    def initialize(address, sessions: 1, max_channels: 64, timeout_ms: 30_000, tls_server_name: nil, tls_ca: nil)
      @h = if tls_server_name
             Lib.beep_client_new_tls(address, sessions, max_channels, timeout_ms, tls_server_name, tls_ca)
           else
             Lib.beep_client_new(address, sessions, max_channels, timeout_ms)
           end
      raise Error, E_ARG if @h.null?

      ObjectSpace.define_finalizer(self, self.class.finalizer(@h))
    end

    def self.finalizer(h) = proc { Lib.beep_client_free(h) }

    def call(method, target, headers: {}, body: '')
      hs = headers.map { |k, v| "#{k}: #{v}" }.join("\n")
      body = body.to_s.b
      out = Fiddle::Pointer.malloc(Fiddle::SIZEOF_VOIDP, Fiddle::RUBY_FREE)
      rc = Lib.beep_call(@h, method.to_s, target.to_s, hs, body, body.bytesize, out)
      raise Error, rc unless rc.zero?

      resp = Fiddle::Pointer.new(out[0, Fiddle::SIZEOF_VOIDP].unpack1('J'))
      begin
        len = Fiddle::Pointer.malloc(Fiddle::SIZEOF_SIZE_T, Fiddle::RUBY_FREE)
        ptr = Lib.beep_response_body(resp, len)
        n = len[0, Fiddle::SIZEOF_SIZE_T].unpack1('J')
        data = n.zero? ? +'' : ptr[0, n]
        headers = Lib.beep_response_headers(resp).to_s.split("\r\n").filter_map do |l|
          k, v = l.split(':', 2)
          [k.strip, v.to_s.strip] if v
        end
        Response.new(Lib.beep_response_status(resp), headers, data)
      ensure
        Lib.beep_response_free(resp)
      end
    end

    def get(target, **kw) = call('GET', target, **kw)
    def post(target, body, **kw) = call('POST', target, body: body, **kw)
  end
end
