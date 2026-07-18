# Dingo Operator — Agent Standards

Coding and review standards for all agents (coding, review, and everything in
between) working on `dingo-operator`. For the working guide (architecture,
commands, toolchain) see [CLAUDE.md](CLAUDE.md).

## Ground rules

- **Do not commit or push unless explicitly asked.** When asked, use
  Conventional Commits and GPG-sign (required for all Blink repos except
  `skunkworks`).
- Match the surrounding code: comment density, naming, and idiom.
- Keep units small and single-purpose. Resource builders in `internal/resources`
  are **pure functions** of the `DingoNode` spec — keep them that way so they
  stay unit-testable without a cluster.

## Coding standards

- **Go 1.26**, module `github.com/blinklabs-io/dingo-operator`. Build with
  `go1.26.1` (see CLAUDE.md).
- **Apache 2.0 header** (13 lines, no SPDX, `// Copyright 2026 Blink Labs
  Software`) at the top of every `.go` file.
- **Formatting**: `golangci-lint fmt` then `golines --max-len=80`. Never leave a
  file unformatted.
- **Lint must be clean**: `make lint` (golangci-lint + nilaway + modernize) with
  zero issues. Notably:
  - No deprecated APIs (staticcheck **SA1019** is enforced).
  - Use `strconv` over `fmt.Sprintf("%d", ...)` (perfsprint).
  - Guard int→int32 conversions (gosec G115); prefer widening or a bounded type.
- **Errors**: wrap with `%w` and context (`fmt.Errorf("apply statefulset: %w",
  err)`). Non-fatal, best-effort operations (e.g. scraping node metrics) must
  not fail a reconcile — log at V(1) and continue.
- **Kubernetes objects**: set a controller owner reference on every child object
  (`ctrl.SetControllerReference`) so garbage collection works. Respect immutable
  fields on update (Service `clusterIP`, StatefulSet selector/serviceName/
  volumeClaimTemplates, PDB selector) — see `internal/controller/apply.go`.
- **Generated code**: after any `api/` change run `make manifests generate` and
  include the regenerated `config/crd`, `config/rbac`, and
  `zz_generated.deepcopy.go` in the same change.

## Testing standards

- Table-driven tests with `testify` (`assert`/`require`). Name subtests with
  `t.Run`.
- Controller tests use `envtest`; gate them so `go test ./...` passes without
  `KUBEBUILDER_ASSETS`.
- New behavior needs coverage. Prefer testing the pure builders directly; use
  envtest only for reconcile/integration behavior.

## Security review checklist (this is a block-production operator)

- **Cold keys must never enter the cluster.** Only KES (hot), VRF, and the
  opcert are handled in-cluster. Opcert issuance delegates cold signing to a
  pluggable signer (Bursa). Reject any change that reads a cold key into the
  operator or a node pod outside the explicit dev-only `secret` signer path.
- **Exactly one active block producer.** Any change to HA/failover must preserve
  the single-active invariant (never two forgers). `ActiveStandby` promotion
  must be fenced (old primary confirmed down/keyless) before the standby is
  keyed.
- **OpCert counter safety.** The next opcert counter must be derived from the
  authoritative on-chain value (via the node), never a naive local increment —
  guard against `CounterOverIncrementedOCERT`.
- **Key material**: mount as files (mode `0400`/`0600`), never env vars; correct
  UID so Dingo's file-mode check passes; least-privilege RBAC on Secrets.
- **Least privilege**: no wildcard RBAC verbs/resources; namespaced Roles where
  possible. Default-deny NetworkPolicy for block producers.
- **Workflows**: SHA-pin all GitHub Actions; never interpolate untrusted event
  input into `run:` scripts.

## Review checklist (general)

- Does it build (`make build`), lint clean (`make lint`), and pass tests
  (`make test`)?
- Are generated manifests regenerated and consistent with `api/` changes?
- Are immutable Kubernetes fields handled on update?
- Are errors wrapped with context; are best-effort paths non-fatal?
- Does the change match Blink conventions (header, formatting, commit style)?
- For CRD/spec changes: is the change backward-compatible, or does it need a new
  API version?
