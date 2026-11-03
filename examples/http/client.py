"""No MINTA package is needed: Python's standard library is sufficient."""
import json
import os
import urllib.request

url = os.environ.get("MINTA_GATEWAY", "http://127.0.0.1:8080") + "/v1/call"
payload = {"method": "POST", "target": "/echo", "json": {"hello": "python"}}
req = urllib.request.Request(url, json.dumps(payload).encode(), {"Content-Type": "application/json"})
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open(req, timeout=10) as response:
    reply = json.load(response)
assert reply["status"] == 200 and json.loads(reply["body"]) == payload["json"], reply
print("Python HTTP/JSON: all tests passed")
