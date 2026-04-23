#include "textflag.h"

// func getg() uintptr
//
// Returns the address of the current goroutine's g struct by reading
// the thread-local storage (TLS) slot where the Go runtime stores it.
// This is the same mechanism the runtime itself uses (e.g., getg() in
// asm_amd64.s).
//
// NOSPLIT because this must not grow the stack (it reads the g pointer
// that controls stack growth).
TEXT ·getg(SB),NOSPLIT,$0-8
    MOVQ (TLS), AX
    MOVQ AX, ret+0(FP)
    RET
