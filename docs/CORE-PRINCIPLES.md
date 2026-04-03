# CORE PRINCIPLES — Required Reading

**This document defines the architectural rules that MUST be followed. Violations are not acceptable.**

---

## #1 Module Isolation is Entropy Control

### Why This Matters

**Agent context limit is 200k tokens.** If a bug spans multiple tightly-coupled modules, the context overflows and the agent loses the ability to trace the issue.

Module isolation is not optional — it's survival.

### What "Works" Looks Like

✅ **Enforces module isolation**: each module must be independently testable, only depend on other modules through interfaces (mockable), never depend on `internal` details of other modules

✅ **Required structure for new modules**:
```
{mod}/
├── api/api.go        # Public interface (ONLY this is public)
├── internal/         # Private implementation & tests
├── docs/             # Documentation for development
└── {mod}.go          # Re-export api.go
```

✅ **Catches cross-module coupling early** before it becomes a context explosion

### What "Fails" Looks Like

❌ Allows module boundaries to blur (internal dependencies across modules)

❌ Lets the agent design without defining clear interfaces

❌ Ignores missing `api/api.go` when a new module is being created

❌ Module A imports Module B's `internal/` directly

---

## #2 Interface-First Design

**Every module interaction MUST go through interfaces defined in `api/api.go`.**

- Interfaces are the contract between modules
- Interfaces must be mockable for testing
- Implementation details are private to the module

---

## #3 No Cross-Module Internal Dependencies

**Module A can NEVER import Module B's `internal/` package.**

```
❌ BAD: github.com/user/project/module-a/internal → imports → github.com/user/project/module-b/internal

✅ GOOD: github.com/user/project/module-a → imports → github.com/user/project/module-b/api
```

---

## #4 Layer Violations are Fatal

**Each layer can only depend on layers below it.**

```
API Layer → Index Layer → Storage Layer → OS/File System
           ↓
      External Value Store
```

A violation (e.g., Storage Layer calling API Layer) creates circular dependencies and context explosion.

---

## #5 Every Module Must Be Independently Testable

**If a module cannot be tested without starting the entire system, the design has failed.**

- Each module has its own tests in `internal/`
- Tests use mocked interfaces for dependencies
- Integration tests are explicit and minimal

---

## Enforcement

These principles are enforced by:
1. **Code review**: Check for `internal/` imports across module boundaries
2. **Architecture tests**: Automated checks for layer violations
3. **Agent instructions**: Every agent working on this project MUST read this document first

---

*Last Updated: 2024*
*Status: MANDATORY — No exceptions*
