# Security

The [versioned threat model](docs/threat-model-v1.md) describes the package
trust boundaries and residual deployment risks.

Report vulnerabilities privately through the repository security process.
Do not include credentials, payloads, authorization material, or production
broker addresses in reports, tests, logs, or fixtures.

TLS verification cannot be disabled by this module. Authentication mechanisms
must obtain secrets from the caller's runtime secret boundary. Topic and group
names are operational identifiers and still require redaction review before
external telemetry export.
