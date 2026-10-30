# Contributing

Thanks for your interest in durably. Issues and pull requests are welcome.

## Getting started

```bash
nvm use            # Node 22 (see .nvmrc); Node 24 is also supported
npm ci
npm run verify     # exactly what CI runs
```

## Ground rules

- **Keep `core` pure.** `src/core` holds the domain model and must not do I/O or import from
  `runtime` or `storage`.
- **Every behaviour change needs a test.** Engine-level suites use `describe.each(storeFactories)`
  so they run against every store. Prefer `ManualClock` and `runUntilIdle()` over real timers.
- **New store adapters** must pass `test/store-contract.ts` unchanged. Put workflow semantics in
  `src/storage/store-rules.ts`, not in the adapter.
- **History is a public contract.** Changing an event's shape breaks replay of existing instances;
  add new event types or optional fields instead, and note it in the CHANGELOG.
- **Record significant decisions** as an ADR in `docs/adr`, using the existing format.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`,
`test:`, `refactor:`, `chore:`.
