// beep.hpp — header-only C++17 RAII wrapper over libbeep (core/ffi/include/beep.h).
//
//   beep::Client c("127.0.0.1:10288");
//   auto r = c.call("POST", "/orders", {{"Content-Type", "application/json"}}, R"({"id":7})");
//   std::cout << r.status << " " << r.body << "\n";
//
//   beep::Server s("0.0.0.0:10288", [](const beep::Request& q, beep::Reply& out) {
//       out.status = 200; out.headers.push_back({"Content-Type", "text/plain"}); out.body = "hi " + q.body;
//   });
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "beep.h"

namespace beep {

using Headers = std::vector<std::pair<std::string, std::string>>;

/// Failure of a call; `code` is the BEEP reply code (e.g. 550) for remote errors, `rc` the BEEP_E_* value.
struct Error : std::runtime_error {
    int rc;
    int code;
    Error(int rc_, int code_, const std::string& msg) : std::runtime_error(msg), rc(rc_), code(code_) {}
};

inline Error last_error(int rc) { return Error(rc, beep_last_error_code(), beep_last_error()); }

inline Headers parse_headers(const char* block) {
    Headers out;
    if (!block) return out;
    std::string s(block);
    size_t pos = 0;
    while (pos < s.size()) {
        size_t end = s.find("\r\n", pos);
        if (end == std::string::npos) end = s.size();
        std::string line = s.substr(pos, end - pos);
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            size_t v = colon + 1;
            while (v < line.size() && line[v] == ' ') ++v;
            out.emplace_back(line.substr(0, colon), line.substr(v));
        }
        pos = end + 2;
    }
    return out;
}

inline std::string join_headers(const Headers& h) {
    std::string s;
    for (auto& kv : h) s += kv.first + ": " + kv.second + "\n";
    return s;
}

struct Response {
    int status = 0;
    Headers headers;
    std::string body;  // binary-safe

    const std::string* header(const std::string& name) const {
        for (auto& kv : headers) {
            if (kv.first.size() != name.size()) continue;
            bool eq = true;
            for (size_t i = 0; i < name.size() && eq; ++i) eq = std::tolower((unsigned char)kv.first[i]) == std::tolower((unsigned char)name[i]);
            if (eq) return &kv.second;
        }
        return nullptr;
    }
};

class Client {
public:
    explicit Client(const std::string& address, int sessions = 1, int max_channels = 64, int timeout_ms = 30000)
        : c_(beep_client_new(address.c_str(), sessions, max_channels, timeout_ms), &beep_client_free) {
        if (!c_) throw last_error(BEEP_E_ARG);
    }

    /// TLS tuning profile; ca_file empty → system roots.
    static Client tls(const std::string& address, const std::string& server_name, const std::string& ca_file = "",
                      int sessions = 1, int max_channels = 64, int timeout_ms = 30000) {
        beep_client* raw = beep_client_new_tls(address.c_str(), sessions, max_channels, timeout_ms, server_name.c_str(),
                                               ca_file.empty() ? nullptr : ca_file.c_str());
        if (!raw) throw last_error(BEEP_E_ARG);
        return Client(raw);
    }

    Response call(const std::string& method, const std::string& target, const Headers& headers = {}, const std::string& body = {}) const {
        beep_response* r = nullptr;
        std::string hs = join_headers(headers);
        int rc = beep_call(c_.get(), method.c_str(), target.c_str(), hs.c_str(),
                           reinterpret_cast<const uint8_t*>(body.data()), body.size(), &r);
        if (rc != BEEP_OK) throw last_error(rc);
        std::unique_ptr<beep_response, decltype(&beep_response_free)> guard(r, &beep_response_free);
        Response out;
        out.status = beep_response_status(r);
        out.headers = parse_headers(beep_response_headers(r));
        size_t n = 0;
        const uint8_t* b = beep_response_body(r, &n);
        out.body.assign(reinterpret_cast<const char*>(b), n);
        return out;
    }

private:
    explicit Client(beep_client* raw) : c_(raw, &beep_client_free) {}
    std::unique_ptr<beep_client, decltype(&beep_client_free)> c_;
};

struct Request {
    std::string method, target, body;
    Headers headers;
};

struct Reply {
    int status = 200;
    Headers headers;
    std::string body;
    int error_code = 0;  // non-zero → answer with BEEP ERR
    std::string error_message;
};

using Handler = std::function<void(const Request&, Reply&)>;

class Server {
public:
    Server(const std::string& listen, Handler h) : h_(std::make_unique<Handler>(std::move(h))) {
        s_ = beep_server_start(listen.c_str(), &Server::trampoline, h_.get());
        if (!s_) throw last_error(BEEP_E_ARG);
    }
    Server(const std::string& listen, Handler h, const std::string& cert, const std::string& key)
        : h_(std::make_unique<Handler>(std::move(h))) {
        s_ = beep_server_start_tls(listen.c_str(), &Server::trampoline, h_.get(), cert.c_str(), key.c_str());
        if (!s_) throw last_error(BEEP_E_ARG);
    }
    ~Server() { if (s_) beep_server_stop(s_); }
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    int port() const { return beep_server_port(s_); }

private:
    static void trampoline(void* user, const beep_request* req, beep_reply* reply) {
        auto& h = *static_cast<Handler*>(user);
        Request q;
        q.method = beep_request_method(req);
        q.target = beep_request_target(req);
        q.headers = parse_headers(beep_request_headers(req));
        size_t n = 0;
        const uint8_t* b = beep_request_body(req, &n);
        q.body.assign(reinterpret_cast<const char*>(b), n);
        Reply out;
        try {
            h(q, out);
        } catch (const std::exception& e) {
            beep_reply_error(reply, 554, e.what());
            return;
        }
        if (out.error_code) {
            beep_reply_error(reply, out.error_code, out.error_message.c_str());
            return;
        }
        beep_reply_status(reply, out.status);
        for (auto& kv : out.headers) beep_reply_header(reply, kv.first.c_str(), kv.second.c_str());
        beep_reply_body(reply, reinterpret_cast<const uint8_t*>(out.body.data()), out.body.size());
    }

    std::unique_ptr<Handler> h_;
    beep_server* s_ = nullptr;
};

}  // namespace beep
