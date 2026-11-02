! gfortran beep.f90 test_beep.f90 <beep.dll|-lbeep> -o test_beep && ./test_beep 127.0.0.1:10288
program test_beep
    use beep
    use, intrinsic :: iso_c_binding, only: c_new_line
    implicit none
    type(beep_client_t) :: c
    character(:), allocatable :: body
    character(len=128) :: addr
    integer :: status, failures

    failures = 0
    call get_command_argument(1, addr)
    if (len_trim(addr) == 0) addr = "127.0.0.1:10288"
    call c%open(trim(addr), 5000)

    status = c%call("POST", "/echo", "Content-Type: text/plain" // c_new_line // "X-Test: fortran", "hello from fortran", body)
    if (status /= 200 .or. body /= "hello from fortran") then
        print *, "FAIL: echo", status, body
        failures = failures + 1
    end if

    status = c%call("GET", "/stream/2", "", "", body)
    if (status /= 200 .or. body /= "line 0" // char(10) // "line 1" // char(10)) then
        print *, "FAIL: stream", status, body
        failures = failures + 1
    end if

    status = c%call("GET", "/err/550", "", "", body)
    if (status /= -3 .or. beep_error_code() /= 550) then
        print *, "FAIL: err", status, beep_error_code(), beep_error_message()
        failures = failures + 1
    end if

    status = c%call("GET", "/bytes/100000", "", "", body)
    if (status /= 200 .or. len(body) /= 100000 .or. body(27:27) /= "a") then
        print *, "FAIL: bytes", status, len(body)
        failures = failures + 1
    end if

    call c%close()
    if (failures == 0) then
        print '(a)', "Fortran: all tests passed"
    else
        print '(a,i0,a)', "Fortran: ", failures, " FAILURE(S)"
        error stop 1
    end if
end program test_beep
