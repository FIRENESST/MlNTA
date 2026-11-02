// Kotlin over native BEEP using the pure-Java library (native/java). NOT verified locally (no kotlinc here).
//   kotlinc -cp minta-beep.jar Kotlin.kt -include-runtime -d demo.jar && java -cp demo.jar:minta-beep.jar KotlinKt
import io.minta.beep.Headers
import io.minta.beep.Rpc
import io.minta.beep.RpcClient
import io.minta.beep.RpcServer

fun main() {
    // A Kotlin backend on BEEP
    RpcServer { req -> Rpc.Reply.of(200, Headers.of("Content-Type", "text/plain"), "kotlin saw ${req.method} ${req.target}") }
        .start("127.0.0.1", 0).use { server ->
            RpcClient("127.0.0.1", server.port()).use { c ->
                val r = c.call("POST", "/echo", Headers.of("Content-Type", "text/plain"), "hi".toByteArray())
                println("${r.status} ${r.text()}")
            }
        }
}
