' VB.NET over native BEEP (Minta.Beep). dotnet run -- 127.0.0.1:10288
Imports System.Text
Imports Minta.Beep

Module Program
    Function Main(args As String()) As Integer
        Dim addr = If(args.Length > 0, args(0), "127.0.0.1:10288")
        Dim i = addr.LastIndexOf(":"c)
        Dim client As New RpcClient(addr.Substring(0, i), Integer.Parse(addr.Substring(i + 1)))
        Dim failures = 0
        Dim hs As New Headers()
        hs.Add("Content-Type", "text/plain")
        Dim r = client.CallAsync("POST", "/echo", hs, Encoding.UTF8.GetBytes("hello from VB")).GetAwaiter().GetResult()
        If r.Status <> 200 OrElse r.Text() <> "hello from VB" Then failures += 1 : Console.WriteLine("FAIL: echo " & r.Text())
        r = client.CallAsync("GET", "/stream/2").GetAwaiter().GetResult()
        If r.Text() <> "line 0" & vbLf & "line 1" & vbLf Then failures += 1 : Console.WriteLine("FAIL: stream")
        client.DisposeAsync().AsTask().Wait()
        Console.WriteLine(If(failures = 0, "VB.NET: all tests passed", $"VB.NET: {failures} FAILURE(S)"))
        Return If(failures = 0, 0, 1)
    End Function
End Module
