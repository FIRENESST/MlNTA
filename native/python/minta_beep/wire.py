"""BEEP wire format: frames (RFC 3080 §2.2, RFC 3081 §3.1), MIME entities, channel-0 XML."""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

MSG, RPY, ERR, ANS, NUL, SEQ = "MSG", "RPY", "ERR", "ANS", "NUL", "SEQ"
DATA_TYPES = (MSG, RPY, ERR, ANS, NUL)
MAX_HEADER_LINE = 128
MAX_I31 = 2_147_483_647
MAX_U32 = 4_294_967_295
BEEP_XML = "application/beep+xml"
Headers = List[Tuple[str, str]]


class ProtocolError(Exception):
    """The peer violated RFC 3080/3081; the session must be dropped."""


@dataclass
class Frame:
    type: str
    channel: int
    msgno: int = 0
    more: bool = False
    seqno: int = 0
    ansno: int = 0
    payload: bytes = b""
    # SEQ only
    ackno: int = 0
    window: int = 0


def _num(s: str, limit: int) -> int:
    if not s or len(s) > 10 or not s.isdigit() or not s.isascii():
        raise ProtocolError(f"bad number {s!r} in frame header")
    v = int(s)
    if v > limit:
        raise ProtocolError(f"number {v} out of range")
    return v


def parse_header(line: bytes) -> Frame:
    """Parse a header line (without CRLF). For data frames, `payload` holds the size as bytes count later."""
    try:
        text = line.decode("ascii")
    except UnicodeDecodeError:
        raise ProtocolError("frame header is not ASCII")
    parts = text.split(" ")
    kw = parts[0]
    if kw == SEQ:
        if len(parts) != 4:
            raise ProtocolError(f"malformed SEQ header {text!r}")
        return Frame(SEQ, _num(parts[1], MAX_I31), ackno=_num(parts[2], MAX_U32), window=_num(parts[3], MAX_I31))
    if kw not in DATA_TYPES:
        raise ProtocolError(f"unknown frame keyword {kw!r}")
    if len(parts) != (7 if kw == ANS else 6):
        raise ProtocolError(f"malformed frame header {text!r}")
    if parts[3] not in (".", "*"):
        raise ProtocolError(f"bad continuation indicator in {text!r}")
    f = Frame(kw, _num(parts[1], MAX_I31), _num(parts[2], MAX_I31), parts[3] == "*", _num(parts[4], MAX_U32))
    f.window = _num(parts[5], MAX_I31)  # payload size, stashed until the payload is read
    if kw == ANS:
        f.ansno = _num(parts[6], MAX_I31)
    if kw == NUL and (f.window != 0 or f.more):
        raise ProtocolError("NUL frame must have size 0 and no continuation")
    return f


def encode_frame(ftype: str, channel: int, msgno: int, more: bool, seqno: int, payload: bytes, ansno: int = 0) -> bytes:
    m = "*" if more else "."
    if ftype == ANS:
        head = f"ANS {channel} {msgno} {m} {seqno} {len(payload)} {ansno}\r\n"
    else:
        head = f"{ftype} {channel} {msgno} {m} {seqno} {len(payload)}\r\n"
    return head.encode("ascii") + payload + b"END\r\n"


def encode_seq(channel: int, ackno: int, window: int) -> bytes:
    return f"SEQ {channel} {ackno} {window}\r\n".encode("ascii")


# ---------------------------------------------------------------------------
# MIME entities


def _clean(s: str) -> str:
    return s.replace("\r", "").replace("\n", "")


def encode_entity(headers: Headers, body: bytes = b"") -> bytes:
    head = "".join(f"{_clean(k)}: {_clean(v)}\r\n" for k, v in headers)
    return head.encode("utf-8") + b"\r\n" + body


def split_head(buf: bytes, complete: bool) -> Optional[Tuple[Headers, int]]:
    """Return (headers, body_offset) or None if undecidable yet (see docs/PROTOCOL.md §1.2)."""
    if buf.startswith(b"\r\n"):
        return [], 2
    p = buf.find(b"\r\n")
    if p < 0:
        return ([], 0) if complete else None
    if b":" not in buf[:p]:
        return [], 0
    q = buf.find(b"\r\n\r\n")
    if q < 0:
        return ([], 0) if complete else None
    return _parse_header_block(buf[: q + 2]), q + 4


def _parse_header_block(block: bytes) -> Headers:
    out: Headers = []
    for line in block.decode("utf-8", "replace").split("\r\n"):
        if not line:
            continue
        if line[0] in " \t" and out:
            k, v = out[-1]
            out[-1] = (k, v + " " + line.strip())
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            out.append((k.strip(), v.strip()))
    return out


def parse_entity(payload: bytes) -> Tuple[Headers, bytes]:
    h, off = split_head(payload, True)
    return h, payload[off:]


def get_header(headers: Headers, name: str) -> Optional[str]:
    n = name.lower()
    for k, v in headers:
        if k.lower() == n:
            return v
    return None


# ---------------------------------------------------------------------------
# Channel-0 management XML


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("'", "&apos;").replace('"', "&quot;")


@dataclass
class ProfileEl:
    uri: str
    data: Optional[str] = None
    base64: bool = False

    def xml(self) -> str:
        enc = " encoding='base64'" if self.base64 else ""
        if self.data is None:
            return f"<profile uri='{esc(self.uri)}'{enc} />"
        if "]]>" not in self.data:
            return f"<profile uri='{esc(self.uri)}'{enc}><![CDATA[{self.data}]]></profile>"
        return f"<profile uri='{esc(self.uri)}'{enc}>{esc(self.data)}</profile>"


@dataclass
class Mgmt:
    kind: str  # greeting | start | close | profile | ok | error
    profiles: List[ProfileEl] = field(default_factory=list)
    number: int = 0
    code: int = 200
    message: str = ""
    server_name: Optional[str] = None

    def xml(self) -> str:
        k = self.kind
        if k == "greeting":
            if not self.profiles:
                return "<greeting />"
            return "<greeting>" + "".join(f"<profile uri='{esc(p.uri)}' />" for p in self.profiles) + "</greeting>"
        if k == "start":
            sn = f" serverName='{esc(self.server_name)}'" if self.server_name else ""
            return f"<start number='{self.number}'{sn}>" + "".join(p.xml() for p in self.profiles) + "</start>"
        if k == "close":
            if self.message:
                return f"<close number='{self.number}' code='{self.code}'>{esc(self.message)}</close>"
            return f"<close number='{self.number}' code='{self.code}' />"
        if k == "profile":
            return self.profiles[0].xml()
        if k == "ok":
            return "<ok />"
        if k == "error":
            return f"<error code='{self.code}'>{esc(self.message)}</error>"
        raise ValueError(k)


def _profile_from(el: ET.Element) -> ProfileEl:
    text = (el.text or "").strip()
    return ProfileEl(el.get("uri", ""), text or None, el.get("encoding") == "base64")


def parse_mgmt(xml: bytes) -> Mgmt:
    if re.search(rb"<!\s*(DOCTYPE|ENTITY)", xml, re.I):
        raise ProtocolError("channel 0: DOCTYPE/ENTITY declarations are not allowed")
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as e:
        raise ProtocolError(f"channel 0: bad XML: {e}")

    def num(name: str, default: Optional[int] = None) -> int:
        v = root.get(name)
        if v is None:
            if default is None:
                raise ProtocolError(f"channel 0: <{root.tag}> lacks {name}")
            return default
        try:
            return int(v.strip())
        except ValueError:
            raise ProtocolError(f"channel 0: bad {name} attribute {v!r}")

    tag = root.tag
    if tag == "greeting":
        return Mgmt("greeting", [ProfileEl(p.get("uri", "")) for p in root.findall("profile")])
    if tag == "start":
        n = num("number")
        if n > MAX_I31:
            raise ProtocolError("channel 0: channel number out of range")
        return Mgmt("start", [_profile_from(p) for p in root.findall("profile")], number=n, server_name=root.get("serverName"))
    if tag == "close":
        return Mgmt("close", number=num("number"), code=num("code", 200), message=(root.text or "").strip())
    if tag == "profile":
        return Mgmt("profile", [_profile_from(root)])
    if tag == "ok":
        return Mgmt("ok")
    if tag == "error":
        return Mgmt("error", code=num("code", 554), message=(root.text or "").strip())
    raise ProtocolError(f"channel 0: unknown element <{tag}>")


def xml_entity(xml: str) -> bytes:
    return encode_entity([("Content-Type", BEEP_XML)], xml.encode("utf-8"))


def error_payload(code: int, message: str) -> bytes:
    return xml_entity(Mgmt("error", code=code, message=message).xml())


def parse_error_payload(payload: bytes) -> Tuple[int, str]:
    _, body = parse_entity(payload)
    try:
        m = parse_mgmt(body)
        if m.kind == "error":
            return m.code, m.message
    except ProtocolError:
        pass
    return 554, body.decode("utf-8", "replace").strip()
