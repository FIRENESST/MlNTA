// Groovy over native BEEP using the pure-Java library (native/java). NOT verified locally.
//   groovy -cp minta-beep.jar groovy.groovy
import io.minta.beep.*

def server = new RpcServer({ req -> Rpc.Reply.of(200, Headers.of('Content-Type', 'text/plain'), "groovy saw ${req.method} ${req.target}".toString()) } as Rpc.Handler)
        .start('127.0.0.1', 0)
new RpcClient('127.0.0.1', server.port()).withCloseable { c ->
    def r = c.call('GET', '/groovy')
    println "${r.status} ${r.text()}"
}
server.close()
