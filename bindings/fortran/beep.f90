! beep.f90 — Fortran 2003 binding to libbeep via ISO_C_BINDING.
!
!   use beep
!   type(beep_client_t) :: c
!   character(:), allocatable :: body
!   integer :: status
!   call c%open("127.0.0.1:10288")
!   status = c%call("POST", "/echo", "Content-Type: text/plain", "hello", body)
!   call c%close()
module beep
    use, intrinsic :: iso_c_binding
    implicit none
    private
    public :: beep_client_t, beep_error_message, beep_error_code

    interface
        function c_client_new(address, sessions, max_channels, timeout_ms) bind(C, name="beep_client_new")
            import :: c_ptr, c_char, c_int
            character(kind=c_char), dimension(*), intent(in) :: address
            integer(c_int), value :: sessions, max_channels, timeout_ms
            type(c_ptr) :: c_client_new
        end function

        subroutine c_client_free(client) bind(C, name="beep_client_free")
            import :: c_ptr
            type(c_ptr), value :: client
        end subroutine

        function c_call_simple(client, method, target, headers, body, body_len, out_body, out_len) bind(C, name="beep_call_simple")
            import :: c_ptr, c_char, c_int, c_size_t
            type(c_ptr), value :: client
            character(kind=c_char), dimension(*), intent(in) :: method, target, headers, body
            integer(c_size_t), value :: body_len
            type(c_ptr), intent(out) :: out_body
            integer(c_size_t), intent(out) :: out_len
            integer(c_int) :: c_call_simple
        end function

        subroutine c_free(p) bind(C, name="beep_free")
            import :: c_ptr
            type(c_ptr), value :: p
        end subroutine

        function c_last_error() bind(C, name="beep_last_error")
            import :: c_ptr
            type(c_ptr) :: c_last_error
        end function

        function c_last_error_code() bind(C, name="beep_last_error_code")
            import :: c_int
            integer(c_int) :: c_last_error_code
        end function

        function c_strlen(s) bind(C, name="strlen")
            import :: c_ptr, c_size_t
            type(c_ptr), value :: s
            integer(c_size_t) :: c_strlen
        end function
    end interface

    type :: beep_client_t
        type(c_ptr) :: handle = c_null_ptr
    contains
        procedure :: open => client_open
        procedure :: call => client_call
        procedure :: close => client_close
    end type

contains

    subroutine client_open(self, address, timeout_ms)
        class(beep_client_t), intent(inout) :: self
        character(*), intent(in) :: address
        integer, intent(in), optional :: timeout_ms
        integer(c_int) :: t
        t = 30000
        if (present(timeout_ms)) t = int(timeout_ms, c_int)
        self%handle = c_client_new(address // c_null_char, 1_c_int, 16_c_int, t)
    end subroutine

    !> Returns the HTTP-style status (>= 100) or a negative BEEP_E_* code.
    !> headers: lines separated by c_new_line ("Name: value").
    function client_call(self, method, target, headers, body, response) result(status)
        class(beep_client_t), intent(in) :: self
        character(*), intent(in) :: method, target, headers, body
        character(:), allocatable, intent(out) :: response
        integer :: status
        type(c_ptr) :: out
        integer(c_size_t) :: n
        character(kind=c_char), pointer :: chars(:)
        integer :: i
        status = c_call_simple(self%handle, method // c_null_char, target // c_null_char, headers // c_null_char, &
                               body, int(len(body), c_size_t), out, n)
        allocate(character(len=int(n)) :: response)
        if (c_associated(out)) then
            call c_f_pointer(out, chars, [n])
            do i = 1, int(n)
                response(i:i) = chars(i)
            end do
            call c_free(out)
        end if
    end function

    subroutine client_close(self)
        class(beep_client_t), intent(inout) :: self
        if (c_associated(self%handle)) call c_client_free(self%handle)
        self%handle = c_null_ptr
    end subroutine

    function beep_error_message() result(msg)
        character(:), allocatable :: msg
        type(c_ptr) :: p
        character(kind=c_char), pointer :: chars(:)
        integer :: i, n
        p = c_last_error()
        n = int(c_strlen(p))
        allocate(character(len=n) :: msg)
        if (n > 0) then
            call c_f_pointer(p, chars, [n])
            do i = 1, n
                msg(i:i) = chars(i)
            end do
        end if
    end function

    integer function beep_error_code()
        beep_error_code = int(c_last_error_code())
    end function

end module beep
