#!/usr/bin/env bash
# Static quality gate. Run locally or in CI:  npm run check
#
#   format-check (Fantomas)  ->  build/type-check  ->  lint (advisory)  ->  integration suite
#
# The integration suite hits the live S2 API and needs ~/.config/s2/config.toml.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$1"; }
fail() { printf '\n\033[1;31mFAILED: %s\033[0m\n' "$1" >&2; exit 1; }

step "Restore dotnet tools"
dotnet tool restore

step "Format check (Fantomas)"
dotnet fantomas --check src tests repl.fsx || fail "Unformatted files. Run: npm run format"

step "Build / type-check"
dotnet build -clp:ErrorsOnly || fail "Build failed."

step "Lint (FSharpLint, advisory)"
dotnet fsharplint lint --lint-config fsharplint.json eff-firegrid.fsproj || echo "lint reported suggestions (advisory, non-blocking)"

step "Integration suite (live S2)"
npm test || fail "Integration suite failed."

printf '\n\033[1;32mAll checks passed.\033[0m\n'
