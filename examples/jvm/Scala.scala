// Scala over native BEEP using the pure-Java library (native/java). Run by tests/run_all.py.
//   scala-cli run Scala.scala --jar minta-beep.jar
import io.minta.beep.{Headers, Rpc, RpcClient, RpcServer}

object BeepScala extends App {
  val server = new RpcServer(req => Rpc.Reply.of(200, Headers.of("Content-Type", "text/plain"), s"scala saw ${req.method} ${req.target}"))
    .start("127.0.0.1", 0)
  val client = new RpcClient("127.0.0.1", server.port())
  val r = client.call("GET", "/scala")
  println(s"${r.status} ${r.text()}")
  client.close()
  server.close()
}
