"""minta_beep — pure-Python RFC 3080/3081 BEEP with the MINTA RPC profile (urn:minta:beep:rpc:1).

Client (asyncio)::

    async with RpcClient("127.0.0.1", 10288) as c:
        r = await c.call("POST", "/orders", json={"id": 7})
        print(r.status, r.json())

Client (blocking)::

    from minta_beep.sync import Client
    print(Client("127.0.0.1", 10288).get("/health").text())

Server::

    async def handler(req: Request) -> ServerResponse:
        return ServerResponse(200, {"Content-Type": "text/plain"}, b"hi " + req.body)
    await (await RpcServer(handler).start("0.0.0.0", 10288)).serve_forever()
"""
from .rpc import (
    RPC_PROFILE, RemoteError, Request, Response, RpcClient, RpcFailure, RpcProfile, RpcServer, RpcTimeout,
    ServerResponse, StreamAborted, StreamingResponse, beep_code_to_http,
)
from .session import (
    TLS_PROFILE, BeepError, Channel, Config, IncomingMessage, Listener, ProfileHandler, Refused, Reply,
    ReplyStream, Responder, Session, SessionClosed, TlsClient, TlsServer,
)
from .wire import ProtocolError

__version__ = "0.1.0"
__all__ = [
    "RPC_PROFILE", "TLS_PROFILE", "RpcClient", "RpcServer", "RpcProfile", "Request", "Response", "ServerResponse",
    "StreamingResponse", "RpcFailure", "RemoteError", "RpcTimeout", "StreamAborted", "beep_code_to_http",
    "BeepError", "SessionClosed", "Refused", "ProtocolError", "Session", "Channel", "Config", "Listener",
    "ProfileHandler", "IncomingMessage", "Responder", "Reply", "ReplyStream", "TlsClient", "TlsServer",
]
