      * beep-demo.cob - GnuCOBOL calling libbeep's simple C ABI.
      * NOT verified locally (no GnuCOBOL here).
      *   cobc -x beep-demo.cob -lbeep && ./beep-demo
      * Mainframe COBOL without dynamic C calls: use the gateway's
      * TCP JSON-lines port (docs/PROTOCOL.md section 3.2) instead.
       IDENTIFICATION DIVISION.
       PROGRAM-ID. BEEP-DEMO.
       DATA DIVISION.
       WORKING-STORAGE SECTION.
       01 WS-ADDR      PIC X(32) VALUE Z"127.0.0.1:10288".
       01 WS-METHOD    PIC X(8)  VALUE Z"POST".
       01 WS-TARGET    PIC X(16) VALUE Z"/echo".
       01 WS-HEADERS   PIC X(32) VALUE Z"Content-Type: text/plain".
       01 WS-BODY      PIC X(16) VALUE "hello from COBOL".
       01 WS-CLIENT    USAGE POINTER.
       01 WS-OUT       USAGE POINTER.
       01 WS-OUT-LEN   USAGE BINARY-C-LONG UNSIGNED.
       01 WS-STATUS    USAGE BINARY-LONG.
       LINKAGE SECTION.
       01 LS-RESP      PIC X(256).
       PROCEDURE DIVISION.
           CALL STATIC "beep_client_new" USING BY REFERENCE WS-ADDR
                BY VALUE 1 BY VALUE 4 BY VALUE 30000
                RETURNING WS-CLIENT
           CALL STATIC "beep_call_simple" USING BY VALUE WS-CLIENT
                BY REFERENCE WS-METHOD BY REFERENCE WS-TARGET
                BY REFERENCE WS-HEADERS BY REFERENCE WS-BODY
                BY VALUE 16 BY REFERENCE WS-OUT BY REFERENCE WS-OUT-LEN
                RETURNING WS-STATUS
           DISPLAY "status: " WS-STATUS
           IF WS-STATUS >= 100
              SET ADDRESS OF LS-RESP TO WS-OUT
              DISPLAY "body: " LS-RESP(1:WS-OUT-LEN)
              CALL STATIC "beep_free" USING BY VALUE WS-OUT
           END-IF
           CALL STATIC "beep_client_free" USING BY VALUE WS-CLIENT
           STOP RUN.
