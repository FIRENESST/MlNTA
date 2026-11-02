package io.minta.beep;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** BEEP wire format: frames (RFC 3080 §2.2, RFC 3081 §3.1), MIME entities, channel-0 XML. */
public final class Wire {
    private Wire() {}

    public static final int MAX_HEADER_LINE = 128;
    static final long MAX_I31 = 2147483647L;
    static final long MAX_U32 = 4294967295L;
    public static final String BEEP_XML = "application/beep+xml";
    private static final byte[] TRAILER = "END\r\n".getBytes(StandardCharsets.US_ASCII);

    static final class Frame {
        String type;
        int channel;
        int msgno;
        boolean more;
        long seqno;
        int ansno;
        byte[] payload;
        long ackno;
        long window;
    }

    private static long num(String s, long max) {
        if (s.isEmpty() || s.length() > 10) throw new BeepException.Protocol("bad number '" + s + "' in frame header");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') throw new BeepException.Protocol("bad number '" + s + "' in frame header");
        }
        long v = Long.parseLong(s);
        if (v > max) throw new BeepException.Protocol("number " + v + " out of range");
        return v;
    }

    /** Read one frame; returns null on a clean EOF between frames. */
    static Frame readFrame(InputStream in, int maxPayload) throws IOException {
        byte[] line = new byte[MAX_HEADER_LINE];
        int n = 0;
        while (true) {
            int b = in.read();
            if (b < 0) {
                if (n == 0) return null;
                throw new BeepException.Protocol("connection closed mid-frame");
            }
            if (n >= MAX_HEADER_LINE) throw new BeepException.Protocol("frame header line too long");
            line[n++] = (byte) b;
            if (n >= 2 && line[n - 2] == '\r' && line[n - 1] == '\n') break;
        }
        for (int i = 0; i < n - 2; i++) {
            if (line[i] < 0x20 || line[i] > 0x7e) throw new BeepException.Protocol("frame header is not ASCII");
        }
        String text = new String(line, 0, n - 2, StandardCharsets.US_ASCII);
        String[] p = text.split(" ", -1);
        Frame f = new Frame();
        f.type = p[0];
        if (f.type.equals("SEQ")) {
            if (p.length != 4) throw new BeepException.Protocol("malformed SEQ header '" + text + "'");
            f.channel = (int) num(p[1], MAX_I31);
            f.ackno = num(p[2], MAX_U32);
            f.window = num(p[3], MAX_I31);
            return f;
        }
        boolean ans = f.type.equals("ANS");
        if (!(f.type.equals("MSG") || f.type.equals("RPY") || f.type.equals("ERR") || ans || f.type.equals("NUL"))) {
            throw new BeepException.Protocol("unknown frame keyword '" + f.type + "'");
        }
        if (p.length != (ans ? 7 : 6)) throw new BeepException.Protocol("malformed frame header '" + text + "'");
        if (!p[3].equals(".") && !p[3].equals("*")) throw new BeepException.Protocol("bad continuation indicator");
        f.channel = (int) num(p[1], MAX_I31);
        f.msgno = (int) num(p[2], MAX_I31);
        f.more = p[3].equals("*");
        f.seqno = num(p[4], MAX_U32);
        int size = (int) num(p[5], MAX_I31);
        if (ans) f.ansno = (int) num(p[6], MAX_I31);
        if (f.type.equals("NUL") && (size != 0 || f.more)) throw new BeepException.Protocol("NUL frame must have size 0");
        if (size > maxPayload) throw new BeepException.Protocol("frame payload " + size + " exceeds limit " + maxPayload);
        byte[] body = readN(in, size + 5);
        for (int i = 0; i < 5; i++) {
            if (body[size + i] != TRAILER[i]) throw new BeepException.Protocol("frame trailer is not END CRLF");
        }
        f.payload = java.util.Arrays.copyOf(body, size);
        return f;
    }

    private static byte[] readN(InputStream in, int n) throws IOException {
        byte[] b = new byte[n];
        int off = 0;
        while (off < n) {
            int r = in.read(b, off, n - off);
            if (r < 0) throw new BeepException.Protocol("connection closed mid-frame");
            off += r;
        }
        return b;
    }

    static byte[] encodeFrame(String type, int channel, int msgno, boolean more, long seqno, byte[] buf, int off, int len, int ansno) {
        String m = more ? "*" : ".";
        String head = type.equals("ANS")
                ? "ANS " + channel + " " + msgno + " " + m + " " + seqno + " " + len + " " + ansno + "\r\n"
                : type + " " + channel + " " + msgno + " " + m + " " + seqno + " " + len + "\r\n";
        byte[] h = head.getBytes(StandardCharsets.US_ASCII);
        byte[] out = new byte[h.length + len + 5];
        System.arraycopy(h, 0, out, 0, h.length);
        System.arraycopy(buf, off, out, h.length, len);
        System.arraycopy(TRAILER, 0, out, h.length + len, 5);
        return out;
    }

    static byte[] encodeSeq(int channel, long ackno, long window) {
        return ("SEQ " + channel + " " + ackno + " " + window + "\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    // ------------------------------------------------------------------ MIME

    private static String clean(String s) {
        return s.indexOf('\r') < 0 && s.indexOf('\n') < 0 ? s : s.replace("\r", "").replace("\n", "");
    }

    public static byte[] encodeEntity(Headers headers, byte[] body) {
        StringBuilder sb = new StringBuilder();
        if (headers != null) {
            for (Map.Entry<String, String> e : headers) sb.append(clean(e.getKey())).append(": ").append(clean(e.getValue())).append("\r\n");
        }
        sb.append("\r\n");
        byte[] h = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] b = body == null ? new byte[0] : body;
        byte[] out = new byte[h.length + b.length];
        System.arraycopy(h, 0, out, 0, h.length);
        System.arraycopy(b, 0, out, h.length, b.length);
        return out;
    }

    static final class Head {
        final Headers headers;
        final int offset;
        Head(Headers h, int off) { headers = h; offset = off; }
    }

    private static int indexOf(byte[] buf, int len, byte[] pat) {
        outer:
        for (int i = 0; i + pat.length <= len; i++) {
            for (int j = 0; j < pat.length; j++) if (buf[i + j] != pat[j]) continue outer;
            return i;
        }
        return -1;
    }

    private static final byte[] CRLF = {'\r', '\n'};
    private static final byte[] CRLF2 = {'\r', '\n', '\r', '\n'};

    /** docs/PROTOCOL.md §1.2; null when more bytes are needed to decide. */
    static Head splitHead(byte[] buf, int len, boolean complete) {
        if (len >= 2 && buf[0] == '\r' && buf[1] == '\n') return new Head(new Headers(), 2);
        int p = indexOf(buf, len, CRLF);
        if (p < 0) return complete ? new Head(new Headers(), 0) : null;
        boolean colon = false;
        for (int i = 0; i < p; i++) if (buf[i] == ':') { colon = true; break; }
        if (!colon) return new Head(new Headers(), 0);
        int q = indexOf(buf, len, CRLF2);
        if (q < 0) return complete ? new Head(new Headers(), 0) : null;
        Headers h = new Headers();
        String block = new String(buf, 0, q + 2, StandardCharsets.UTF_8);
        String lastK = null;
        String lastV = null;
        List<String[]> tmp = new ArrayList<>();
        for (String l : block.split("\r\n")) {
            if (l.isEmpty()) continue;
            if ((l.charAt(0) == ' ' || l.charAt(0) == '\t') && !tmp.isEmpty()) {
                tmp.get(tmp.size() - 1)[1] += " " + l.trim();
                continue;
            }
            int c = l.indexOf(':');
            if (c > 0) tmp.add(new String[] {l.substring(0, c).trim(), l.substring(c + 1).trim()});
        }
        for (String[] e : tmp) h.add(e[0], e[1]);
        return new Head(h, q + 4);
    }

    public static Map.Entry<Headers, byte[]> parseEntity(byte[] payload) {
        Head h = splitHead(payload, payload.length, true);
        return new java.util.AbstractMap.SimpleImmutableEntry<>(h.headers, java.util.Arrays.copyOfRange(payload, h.offset, payload.length));
    }

    // ------------------------------------------------------------------ XML (channel 0)

    static final class El {
        String name;
        final Map<String, String> attrs = new LinkedHashMap<>();
        final List<Object> children = new ArrayList<>();
        String text() {
            StringBuilder sb = new StringBuilder();
            for (Object o : children) if (o instanceof String) sb.append((String) o);
            return sb.toString();
        }
        List<El> elements(String n) {
            List<El> out = new ArrayList<>();
            for (Object o : children) if (o instanceof El && ((El) o).name.equals(n)) out.add((El) o);
            return out;
        }
    }

    /** Minimal XML reader; DOCTYPE is rejected so entity-expansion attacks are impossible. */
    static El parseXml(String s) {
        int[] i = {0};
        misc(s, i);
        El root = element(s, i, 0);
        misc(s, i);
        if (i[0] != s.length()) throw new BeepException.Protocol("trailing content after XML root");
        return root;
    }

    private static void ws(String s, int[] i) {
        while (i[0] < s.length() && Character.isWhitespace(s.charAt(i[0]))) i[0]++;
    }

    private static int skipPast(String s, int[] i, String end) {
        int p = s.indexOf(end, i[0]);
        if (p < 0) throw new BeepException.Protocol("unterminated XML construct, expected " + end);
        int start = i[0];
        i[0] = p + end.length();
        return start;
    }

    private static void misc(String s, int[] i) {
        while (true) {
            ws(s, i);
            if (s.startsWith("<?", i[0])) skipPast(s, i, "?>");
            else if (s.startsWith("<!--", i[0])) skipPast(s, i, "-->");
            else if (s.startsWith("<!", i[0])) throw new BeepException.Protocol("DOCTYPE/declarations are not allowed");
            else return;
        }
    }

    private static String name(String s, int[] i) {
        int start = i[0];
        while (i[0] < s.length()) {
            char c = s.charAt(i[0]);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.' || c == ':') i[0]++;
            else break;
        }
        if (start == i[0]) throw new BeepException.Protocol("expected an XML name");
        return s.substring(start, i[0]);
    }

    private static El element(String s, int[] i, int depth) {
        if (depth > 16) throw new BeepException.Protocol("XML nested too deeply");
        if (!s.startsWith("<", i[0])) throw new BeepException.Protocol("expected '<'");
        i[0]++;
        El el = new El();
        el.name = name(s, i);
        while (true) {
            ws(s, i);
            if (s.startsWith("/>", i[0])) { i[0] += 2; return el; }
            if (s.startsWith(">", i[0])) { i[0]++; break; }
            String an = name(s, i);
            ws(s, i);
            if (!s.startsWith("=", i[0])) throw new BeepException.Protocol("attribute " + an + " lacks '='");
            i[0]++;
            ws(s, i);
            if (i[0] >= s.length()) throw new BeepException.Protocol("unexpected end in attribute");
            char q = s.charAt(i[0]);
            if (q != '\'' && q != '"') throw new BeepException.Protocol("attribute value must be quoted");
            int end = s.indexOf(q, i[0] + 1);
            if (end < 0) throw new BeepException.Protocol("unterminated attribute value");
            el.attrs.put(an, unescape(s.substring(i[0] + 1, end)));
            i[0] = end + 1;
        }
        while (true) {
            if (i[0] >= s.length()) throw new BeepException.Protocol("unterminated element <" + el.name + ">");
            if (s.startsWith("</", i[0])) {
                i[0] += 2;
                String close = name(s, i);
                ws(s, i);
                if (!close.equals(el.name) || !s.startsWith(">", i[0])) throw new BeepException.Protocol("mismatched closing tag </" + close + ">");
                i[0]++;
                return el;
            } else if (s.startsWith("<![CDATA[", i[0])) {
                i[0] += 9;
                int start = skipPast(s, i, "]]>");
                el.children.add(s.substring(start, i[0] - 3));
            } else if (s.startsWith("<!--", i[0])) {
                skipPast(s, i, "-->");
            } else if (s.startsWith("<?", i[0])) {
                skipPast(s, i, "?>");
            } else if (s.startsWith("<!", i[0])) {
                throw new BeepException.Protocol("declarations are not allowed");
            } else if (s.startsWith("<", i[0])) {
                el.children.add(element(s, i, depth + 1));
            } else {
                int p = s.indexOf('<', i[0]);
                int end = p < 0 ? s.length() : p;
                el.children.add(unescape(s.substring(i[0], end)));
                i[0] = end;
            }
        }
    }

    private static String unescape(String s) {
        if (s.indexOf('&') < 0) return s;
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c != '&') { out.append(c); i++; continue; }
            int semi = s.indexOf(';', i);
            if (semi < 0) throw new BeepException.Protocol("unterminated XML entity");
            String e = s.substring(i + 1, semi);
            switch (e) {
                case "lt": out.append('<'); break;
                case "gt": out.append('>'); break;
                case "amp": out.append('&'); break;
                case "quot": out.append('"'); break;
                case "apos": out.append('\''); break;
                default:
                    if (!e.startsWith("#")) throw new BeepException.Protocol("unknown XML entity &" + e + ";");
                    int cp;
                    try {
                        cp = e.startsWith("#x") || e.startsWith("#X") ? Integer.parseInt(e.substring(2), 16) : Integer.parseInt(e.substring(1));
                    } catch (NumberFormatException ex) {
                        throw new BeepException.Protocol("bad character reference");
                    }
                    if (!Character.isValidCodePoint(cp)) throw new BeepException.Protocol("bad character reference");
                    out.appendCodePoint(cp);
            }
            i = semi + 1;
        }
        return out.toString();
    }

    static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("'", "&apos;").replace("\"", "&quot;");
    }

    static final class ProfileEl {
        final String uri;
        final String data;
        ProfileEl(String uri, String data) { this.uri = uri; this.data = data; }
        String xml() {
            if (data == null) return "<profile uri='" + esc(uri) + "' />";
            if (!data.contains("]]>")) return "<profile uri='" + esc(uri) + "'><![CDATA[" + data + "]]></profile>";
            return "<profile uri='" + esc(uri) + "'>" + esc(data) + "</profile>";
        }
    }

    /** Channel-0 management message. kind ∈ greeting, start, close, profile, ok, error. */
    static final class Mgmt {
        String kind;
        List<ProfileEl> profiles = new ArrayList<>();
        int number;
        int code = 200;
        String message = "";
        String serverName;

        static Mgmt of(String kind) {
            Mgmt m = new Mgmt();
            m.kind = kind;
            return m;
        }

        static Mgmt error(int code, String message) {
            Mgmt m = of("error");
            m.code = code;
            m.message = message;
            return m;
        }

        String xml() {
            StringBuilder sb = new StringBuilder();
            switch (kind) {
                case "greeting":
                    if (profiles.isEmpty()) return "<greeting />";
                    sb.append("<greeting>");
                    for (ProfileEl p : profiles) sb.append("<profile uri='").append(esc(p.uri)).append("' />");
                    return sb.append("</greeting>").toString();
                case "start":
                    sb.append("<start number='").append(number).append("'");
                    if (serverName != null) sb.append(" serverName='").append(esc(serverName)).append("'");
                    sb.append(">");
                    for (ProfileEl p : profiles) sb.append(p.xml());
                    return sb.append("</start>").toString();
                case "close":
                    if (message != null && !message.isEmpty()) return "<close number='" + number + "' code='" + code + "'>" + esc(message) + "</close>";
                    return "<close number='" + number + "' code='" + code + "' />";
                case "profile":
                    return profiles.get(0).xml();
                case "ok":
                    return "<ok />";
                case "error":
                    return "<error code='" + code + "'>" + esc(message) + "</error>";
                default:
                    throw new IllegalStateException(kind);
            }
        }

        static Mgmt parse(byte[] body) {
            El root = parseXml(new String(body, StandardCharsets.UTF_8));
            Mgmt m = of(root.name);
            switch (root.name) {
                case "greeting":
                    for (El p : root.elements("profile")) m.profiles.add(new ProfileEl(p.attrs.getOrDefault("uri", ""), null));
                    return m;
                case "start":
                    m.number = attrNum(root, "number", -1);
                    m.serverName = root.attrs.get("serverName");
                    for (El p : root.elements("profile")) m.profiles.add(profileFrom(p));
                    return m;
                case "close":
                    m.number = attrNum(root, "number", -1);
                    m.code = attrNum(root, "code", 200);
                    m.message = root.text().trim();
                    return m;
                case "profile":
                    m.profiles.add(profileFrom(root));
                    return m;
                case "ok":
                    return m;
                case "error":
                    m.code = attrNum(root, "code", 554);
                    m.message = root.text().trim();
                    return m;
                default:
                    throw new BeepException.Protocol("channel 0: unknown element <" + root.name + ">");
            }
        }

        private static ProfileEl profileFrom(El el) {
            String t = el.text().trim();
            return new ProfileEl(el.attrs.getOrDefault("uri", ""), t.isEmpty() ? null : t);
        }

        private static int attrNum(El el, String name, int dflt) {
            String v = el.attrs.get(name);
            if (v == null) {
                if (dflt < 0) throw new BeepException.Protocol("channel 0: <" + el.name + "> lacks " + name);
                return dflt;
            }
            try {
                long n = Long.parseLong(v.trim());
                if (n < 0 || n > MAX_I31) throw new NumberFormatException();
                return (int) n;
            } catch (NumberFormatException e) {
                throw new BeepException.Protocol("channel 0: bad " + name + " attribute");
            }
        }
    }

    static byte[] xmlEntity(String xml) {
        return encodeEntity(Headers.of("Content-Type", BEEP_XML), xml.getBytes(StandardCharsets.UTF_8));
    }

    static byte[] errorPayload(int code, String message) {
        return xmlEntity(Mgmt.error(code, message).xml());
    }

    /** Returns {code, message} parsed from an ERR payload. */
    static Object[] parseErrorPayload(byte[] payload) {
        byte[] body = parseEntity(payload).getValue();
        try {
            Mgmt m = Mgmt.parse(body);
            if (m.kind.equals("error")) return new Object[] {m.code, m.message};
        } catch (BeepException e) {
            // not XML
        }
        return new Object[] {554, new String(body, StandardCharsets.UTF_8).trim()};
    }

    static byte[] concat(List<byte[]> parts) {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        for (byte[] p : parts) bo.write(p, 0, p.length);
        return bo.toByteArray();
    }
}
