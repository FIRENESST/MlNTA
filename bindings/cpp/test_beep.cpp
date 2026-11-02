// g++ -std=c++17 -I../../core/ffi/include -I. test_beep.cpp <beep.dll|-lbeep> -o test_beep && ./test_beep 127.0.0.1:10288
#include <cctype>
#include <iostream>
#include "beep.hpp"

static int failures = 0;
#define CHECK(c, msg) do { if (!(c)) { ++failures; std::cout << "FAIL: " << msg << "\n"; } } while (0)

int main(int argc, char** argv) {
    std::string addr = argc > 1 ? argv[1] : "127.0.0.1:10288";
    beep::Client c(addr, 1, 8, 5000);

    auto r = c.call("POST", "/echo", {{"Content-Type", "text/plain"}, {"X-Test", "cpp"}}, "hello from c++");
    CHECK(r.status == 200 && r.body == "hello from c++", "echo " << r.status << " " << r.body);
    CHECK(r.header("x-test") && *r.header("x-test") == "cpp", "X-Test header");

    r = c.call("GET", "/stream/3");
    CHECK(r.body == "line 0\nline 1\nline 2\n", "stream " << r.body);

    try {
        c.call("GET", "/err/550");
        CHECK(false, "expected beep::Error");
    } catch (const beep::Error& e) {
        CHECK(e.rc == BEEP_E_REMOTE && e.code == 550, "err rc=" << e.rc << " code=" << e.code);
    }

    std::string bin("a\0b\0c", 5);
    r = c.call("PUT", "/echo", {}, bin);
    CHECK(r.body == bin, "binary-safe body");

    beep::Server s("127.0.0.1:0", [](const beep::Request& q, beep::Reply& out) {
        if (q.target == "/boom") throw std::runtime_error("c++ handler threw");
        out.status = 201;
        out.headers.push_back({"X-Served-By", "cpp"});
        out.body = q.method + " " + q.target + " " + q.body;
    });
    beep::Client c2("127.0.0.1:" + std::to_string(s.port()));
    r = c2.call("PATCH", "/cpp", {}, "xyz");
    CHECK(r.status == 201 && r.body == "PATCH /cpp xyz" && r.header("X-Served-By"), "c++ server " << r.body);
    try {
        c2.call("GET", "/boom");
        CHECK(false, "expected handler error");
    } catch (const beep::Error& e) {
        CHECK(e.code == 554 && std::string(e.what()).find("c++ handler threw") != std::string::npos, "boom " << e.what());
    }

    std::cout << (failures ? "C++: FAILURES\n" : "C++: all tests passed\n");
    return failures ? 1 : 0;
}
