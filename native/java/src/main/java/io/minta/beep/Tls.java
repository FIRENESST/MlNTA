package io.minta.beep;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/** TLS tuning profile (RFC 3080 §3.1, http://iana.org/beep/TLS). */
public final class Tls {
    private Tls() {}

    public static final String PROFILE = "http://iana.org/beep/TLS";

    /** Client side: verify {@code serverName}; trust {@code caFile} (PEM) or the JVM default trust store. */
    public static final class Client {
        public final String serverName;
        public final String caFile;
        public final SSLContext context;

        public Client(String serverName, String caFile) {
            this(serverName, caFile, null);
        }

        public Client(String serverName, String caFile, SSLContext context) {
            this.serverName = serverName;
            this.caFile = caFile;
            this.context = context;
        }

        SSLContext ssl() throws Exception {
            if (context != null) return context;
            if (caFile == null) return SSLContext.getDefault();
            KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
            ts.load(null, null);
            int i = 0;
            for (Certificate c : readCerts(caFile)) ts.setCertificateEntry("ca" + i++, c);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
            return ctx;
        }
    }

    /** Server side: PEM certificate chain and PKCS#8 PEM private key (RSA, EC or Ed25519). */
    public static final class Server {
        public final String certFile;
        public final String keyFile;
        public final SSLContext context;

        public Server(String certFile, String keyFile) {
            this(certFile, keyFile, null);
        }

        public Server(String certFile, String keyFile, SSLContext context) {
            this.certFile = certFile;
            this.keyFile = keyFile;
            this.context = context;
        }

        SSLContext ssl() throws Exception {
            if (context != null) return context;
            Collection<? extends Certificate> certs = readCerts(certFile);
            PrivateKey key = readKey(keyFile);
            char[] pass = "minta".toCharArray();
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);
            ks.setKeyEntry("key", key, pass, certs.toArray(new Certificate[0]));
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, pass);
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), null, null);
            return ctx;
        }
    }

    static Collection<? extends Certificate> readCerts(String file) throws Exception {
        try (InputStream in = Files.newInputStream(Paths.get(file))) {
            return CertificateFactory.getInstance("X.509").generateCertificates(in);
        }
    }

    static PrivateKey readKey(String file) throws Exception {
        String pem = new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.US_ASCII);
        if (pem.contains("BEGIN RSA PRIVATE KEY") || pem.contains("BEGIN EC PRIVATE KEY")) {
            throw new IllegalArgumentException("convert the key to PKCS#8: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key8.pem");
        }
        String b64 = pem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(b64));
        for (String alg : new String[] {"RSA", "EC", "Ed25519", "EdDSA"}) {
            try {
                return KeyFactory.getInstance(alg).generatePrivate(spec);
            } catch (Exception ignored) {
                // try the next algorithm
            }
        }
        throw new IllegalArgumentException("unsupported private key in " + file);
    }

    /** Frame I/O on the raw socket before the real session (reads never over-consume). */
    private static final class Boot {
        final InputStream in;
        final OutputStream out;
        final Map<Integer, Long> seq = new HashMap<>();

        Boot(Socket s) throws IOException {
            in = s.getInputStream();
            out = s.getOutputStream();
        }

        void send(String type, int channel, int msgno, String xml) throws IOException {
            byte[] p = Wire.xmlEntity(xml);
            long s = seq.getOrDefault(channel, 0L);
            out.write(Wire.encodeFrame(type, channel, msgno, false, s, p, 0, p.length, 0));
            out.flush();
            seq.put(channel, (s + p.length) & Session.MASK);
        }

        Object[] recv() throws IOException {
            java.io.ByteArrayOutputStream acc = new java.io.ByteArrayOutputStream();
            while (true) {
                Wire.Frame f = Wire.readFrame(in, 65536);
                if (f == null) throw new BeepException.Closed("connection closed during TLS negotiation");
                if (f.type.equals("SEQ")) continue;
                acc.write(f.payload, 0, f.payload.length);
                if (!f.more) return new Object[] {f.type, f.channel, f.msgno, Wire.parseEntity(acc.toByteArray()).getValue()};
            }
        }
    }

    static Socket clientBootstrap(Socket s, String host, int port, Client opts) throws IOException {
        Boot b = new Boot(s);
        b.send("RPY", 0, 0, "<greeting />");
        Object[] r = b.recv();
        Wire.Mgmt g = Wire.Mgmt.parse((byte[]) r[3]);
        if (!"RPY".equals(r[0]) || !g.kind.equals("greeting")) throw new BeepException.Protocol("expected greeting");
        boolean offered = false;
        for (Wire.ProfileEl p : g.profiles) offered |= p.uri.equals(PROFILE);
        if (!offered) throw new BeepException("peer does not offer the TLS profile");
        Wire.Mgmt start = Wire.Mgmt.of("start");
        start.number = 1;
        start.serverName = opts.serverName;
        start.profiles.add(new Wire.ProfileEl(PROFILE, "<ready />"));
        b.send("MSG", 0, 1, start.xml());
        r = b.recv();
        Wire.Mgmt m = Wire.Mgmt.parse((byte[]) r[3]);
        if (m.kind.equals("error")) throw new BeepException.Refused(m.code, m.message);
        if (!"RPY".equals(r[0]) || !m.kind.equals("profile") || m.profiles.get(0).data == null || !m.profiles.get(0).data.contains("<proceed")) {
            throw new BeepException("peer did not proceed with TLS");
        }
        try {
            SSLSocket ssl = (SSLSocket) opts.ssl().getSocketFactory().createSocket(s, opts.serverName, port, true);
            ssl.setUseClientMode(true);
            SSLParameters params = ssl.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");
            params.setServerNames(Collections.singletonList(new SNIHostName(opts.serverName)));
            ssl.setSSLParameters(params);
            ssl.startHandshake();
            return ssl;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("tls: " + e.getMessage(), e);
        }
    }

    static Socket serverBootstrap(Socket s, Server opts) throws IOException {
        Boot b = new Boot(s);
        Wire.Mgmt greet = Wire.Mgmt.of("greeting");
        greet.profiles.add(new Wire.ProfileEl(PROFILE, null));
        b.send("RPY", 0, 0, greet.xml());
        Object[] r = b.recv();
        if (!"RPY".equals(r[0]) || (Integer) r[1] != 0) throw new BeepException.Protocol("expected greeting");
        Integer tlsChannel = null;
        boolean proceeded = false;
        for (int i = 0; i < 16 && !proceeded; i++) {
            r = b.recv();
            int ch = (Integer) r[1];
            int msgno = (Integer) r[2];
            byte[] body = (byte[]) r[3];
            if (!"MSG".equals(r[0])) throw new BeepException.Protocol("expected MSG during TLS negotiation");
            if (tlsChannel != null && ch == tlsChannel) {
                if (new String(body, StandardCharsets.UTF_8).contains("<ready")) {
                    b.send("RPY", ch, msgno, "<proceed />");
                    proceeded = true;
                } else {
                    b.send("ERR", ch, msgno, Wire.Mgmt.error(500, "expected <ready />").xml());
                }
                continue;
            }
            Wire.Mgmt m = Wire.Mgmt.parse(body);
            Wire.ProfileEl p = null;
            if (m.kind.equals("start") && m.number % 2 == 1) {
                for (Wire.ProfileEl x : m.profiles) if (x.uri.equals(PROFILE)) p = x;
            }
            if (p != null) {
                Wire.Mgmt reply = Wire.Mgmt.of("profile");
                if (p.data != null && p.data.contains("<ready")) {
                    reply.profiles.add(new Wire.ProfileEl(PROFILE, "<proceed />"));
                    proceeded = true;
                } else {
                    reply.profiles.add(new Wire.ProfileEl(PROFILE, null));
                    tlsChannel = m.number;
                }
                b.send("RPY", 0, msgno, reply.xml());
            } else if (m.kind.equals("close") && m.number == 0) {
                b.send("RPY", 0, msgno, "<ok />");
                throw new BeepException.Closed("peer released session before TLS");
            } else {
                b.send("ERR", 0, msgno, Wire.Mgmt.error(538, "TLS required").xml());
            }
        }
        if (!proceeded) throw new BeepException("peer never requested TLS");
        try {
            SSLSocket ssl = (SSLSocket) opts.ssl().getSocketFactory().createSocket(s, null, s.getPort(), true);
            ssl.setUseClientMode(false);
            ssl.startHandshake();
            return ssl;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("tls: " + e.getMessage(), e);
        }
    }
}
