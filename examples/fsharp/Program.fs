// F# over native BEEP (Minta.Beep). dotnet run -- 127.0.0.1:10288
open System
open System.Text
open System.Threading.Tasks
open Minta.Beep

[<EntryPoint>]
let main argv =
    let addr = if argv.Length > 0 then argv.[0] else "127.0.0.1:10288"
    let host, port = let i = addr.LastIndexOf ':' in addr.[.. i - 1], int addr.[i + 1 ..]
    let mutable failures = 0
    let check cond msg = if not cond then failures <- failures + 1; printfn "FAIL: %s" msg
    task {
        let client = RpcClient(host, port)
        let hs = Headers()
        hs.Add("Content-Type", "text/plain") |> ignore
        let! r = client.CallAsync("POST", "/echo", hs, Encoding.UTF8.GetBytes "hello from F#")
        check (r.Status = 200 && r.Text() = "hello from F#") (sprintf "echo %d %s" r.Status (r.Text()))
        try
            let! _ = client.CallAsync("GET", "/err/537")
            check false "expected RemoteException"
        with :? RemoteException as e -> check (e.Code = 537) "err code"
        // An F# backend served over BEEP
        use server =
            RpcServer(fun req -> Task.FromResult(RpcReply.Of(200, null, sprintf "F# saw %s %s" req.Method req.Target)))
                .Start("127.0.0.1", 0)
        let c2 = RpcClient("127.0.0.1", server.Port)
        let! r2 = c2.CallAsync("GET", "/fsharp")
        check (r2.Text() = "F# saw GET /fsharp") (r2.Text())
        do! c2.DisposeAsync()
        do! client.DisposeAsync()
    } |> fun t -> t.Wait()
    printfn "%s" (if failures = 0 then "F#: all tests passed" else sprintf "F#: %d FAILURE(S)" failures)
    if failures = 0 then 0 else 1
