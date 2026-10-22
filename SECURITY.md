# Security

The [versioned threat model](docs/threat-model-v1.md) describes the package
trust boundaries and residual deployment risks.

Security fixes apply to the latest v2 release. The latest v1 release also
receives security fixes until the `adapters/mskiam`, `adapters/otel` and
`adapters/service` modules publish majors built on root v2.

Report vulnerabilities privately through the repository security process.
Do not include credentials, payloads, authorization material, or production
broker addresses in reports, tests, logs, or fixtures.

TLS verification cannot be disabled by this module. Authentication mechanisms
must obtain secrets from the caller's runtime secret boundary. Topic and group
names are operational identifiers and still require redaction review before
external telemetry export.
