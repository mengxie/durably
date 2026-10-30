# Security policy

Please report suspected vulnerabilities privately through
[GitHub security advisories](https://github.com/mengxie/durably/security/advisories/new)
rather than in a public issue. You can expect an initial response within a week.

Workflow inputs, step results and signal payloads are stored as plain JSON in the configured
store. Do not put secrets in them; pass references (such as a secret-manager key) instead.
