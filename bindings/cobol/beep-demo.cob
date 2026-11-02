      * beep-demo.cob - GnuCOBOL calling libbeep's simple C ABI.
      *   cobc -x beep-demo.cob -lbeep
      *   BEEP_ECHO=127.0.0.1:10288 ./beep-demo
      * Mainframe COBOL without dynamic C calls: use the gateway's
      * TCP JSON-lines port (docs/PROTOCOL.md section 3.2) instead.
       IDENTIFICATION DIVISION.
       PROGRAM-ID. BEEP-DEMO.
       DATA DIVISION.
       WORKING-STORAGE SECTION.
       01 WS-ENV       PIC X(64).
       01 WS-ADDR      PIC X(65).
       01 WS-METHOD    PIC X(8)  VALUE Z"POST".
       01 WS-TARGET    PIC X(16) VALUE Z"/echo".
       01 WS-GET       PIC X(8)  VALUE Z"GET".
       01 WS-ERR       PIC X(16) VALUE Z"/err/537".
       01 WS-HEADERS   PIC X(32) VALUE Z"Content-Type: text/plain".
       01 WS-NOHDR     PIC X(1)  VALUE LOW-VALUE.
       01 WS-BODY      PIC X(16) VALUE "hello from COBOL".
       01 WS-SESSIONS  USAGE BINARY-LONG VALUE 1.
       01 WS-CHANNELS  USAGE BINARY-LONG VALUE 4.
       01 WS-TIMEOUT   USAGE BINARY-LONG VALUE 5000.
       01 WS-BODY-LEN  USAGE BINARY-C-LONG UNSIGNED VALUE 16.
       01 WS-ZERO-LEN  USAGE BINARY-C-LONG UNSIGNED VALUE 0.
       01 WS-CLIENT    USAGE POINTER.
       01 WS-OUT       USAGE POINTER.
       01 WS-OUT-LEN   USAGE BINARY-C-LONG UNSIGNED.
       01 WS-STATUS    USAGE BINARY-LONG.
       01 WS-CODE      USAGE BINARY-LONG.
       01 WS-FAILURES  PIC 9(2) VALUE 0.
       LINKAGE SECTION.
       01 LS-RESP      PIC X(256).
       PROCEDURE DIVISION.
           ACCEPT WS-ENV FROM ENVIRONMENT "BEEP_ECHO"
           IF WS-ENV = SPACES
              MOVE "127.0.0.1:10288" TO WS-ENV
           END-IF
           STRING FUNCTION TRIM(WS-ENV) DELIMITED BY SIZE
                  LOW-VALUE DELIMITED BY SIZE INTO WS-ADDR
           CALL STATIC "beep_client_new" USING BY REFERENCE WS-ADDR
                BY VALUE WS-SESSIONS BY VALUE WS-CHANNELS
                BY VALUE WS-TIMEOUT
                RETURNING WS-CLIENT
           IF WS-CLIENT = NULL
              DISPLAY "FAIL: client"
              STOP RUN RETURNING 1
           END-IF

           CALL STATIC "beep_call_simple" USING BY VALUE WS-CLIENT
                BY REFERENCE WS-METHOD BY REFERENCE WS-TARGET
                BY REFERENCE WS-HEADERS BY REFERENCE WS-BODY
                BY VALUE WS-BODY-LEN BY REFERENCE WS-OUT
                BY REFERENCE WS-OUT-LEN
                RETURNING WS-STATUS
           DISPLAY "status: " WS-STATUS
           IF WS-STATUS = 200 AND WS-OUT-LEN = 16
              SET ADDRESS OF LS-RESP TO WS-OUT
              DISPLAY "body: " LS-RESP(1:WS-OUT-LEN)
              IF LS-RESP(1:16) NOT = "hello from COBOL"
                 DISPLAY "FAIL: echo body"
                 ADD 1 TO WS-FAILURES
              END-IF
              CALL STATIC "beep_free" USING BY VALUE WS-OUT
           ELSE
              DISPLAY "FAIL: echo status/length"
              ADD 1 TO WS-FAILURES
           END-IF

           CALL STATIC "beep_call_simple" USING BY VALUE WS-CLIENT
                BY REFERENCE WS-GET BY REFERENCE WS-ERR
                BY REFERENCE WS-NOHDR BY REFERENCE WS-BODY
                BY VALUE WS-ZERO-LEN BY REFERENCE WS-OUT
                BY REFERENCE WS-OUT-LEN
                RETURNING WS-STATUS
           CALL STATIC "beep_last_error_code" RETURNING WS-CODE
           IF WS-STATUS NOT = -3 OR WS-CODE NOT = 537
              DISPLAY "FAIL: err " WS-STATUS " " WS-CODE
              ADD 1 TO WS-FAILURES
           END-IF

           CALL STATIC "beep_client_free" USING BY VALUE WS-CLIENT
      * Calls to void C functions leave junk in RETURN-CODE, which
      * STOP RUN would use as the exit status.
           IF WS-FAILURES = 0
              DISPLAY "COBOL (FFI): all tests passed"
              MOVE 0 TO RETURN-CODE
              STOP RUN
           END-IF
           DISPLAY "COBOL (FFI): " WS-FAILURES " FAILURE(S)"
           STOP RUN RETURNING 1.
