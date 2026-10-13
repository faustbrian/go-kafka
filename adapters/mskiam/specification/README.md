# Specification conformance matrix

[Documentation map](../../../docs/index.md) · [Repository README](../../../README.md)
[Next: Documentation map](../../../docs/index.md)

The [decision register](../docs/specification-decisions.md) binds every observable decision to pinned authorities and current evidence.

| Decision | Authority | Executable evidence | Differential evidence |
| --- | --- | --- | --- |
| KAFKA-MSKIAM-DEC-001 | `aws-msk-signer-104` | `TestProviderGeneratesOwnedExpiringMSKIAMToken`, `TestSignedTokenValidationRejectsEachMalformedField` | `adapters/mskiam/specification/maintained-peer-differential.md` |
| KAFKA-MSKIAM-DEC-002 | `aws-sdk-go-v2-1430` | `TestProviderRefreshesExpiringCredentialsAndCapsTokenExpiry`, `TestConcurrentNearExpiryCredentialsRefreshOnce` | `adapters/mskiam/specification/maintained-peer-differential.md` |
| KAFKA-MSKIAM-DEC-003 | `aws-msk-guide` | `TestNewUsesAWSDefaultCredentialChain` | `adapters/mskiam/specification/maintained-peer-differential.md` |
