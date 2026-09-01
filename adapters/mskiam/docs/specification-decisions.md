# Specification decisions

This register is authoritative together with `specification/decisions.json`. Source and update authorities are pinned independently in `specification/monitoring.json`.

## KAFKA-MSKIAM-DEC-001: AWS owns signing while the adapter validates output

- **status:** resolved
- **owner:** Kafka MSK IAM adapter maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** AWS MSK IAM SASL/OAUTHBEARER signer contract v1.0.4
- **version:** AWS MSK IAM signer v1.0.4
- **source_authority:** aws-msk-signer-104
- **section:** GenerateAuthToken and SASL/OAUTHBEARER integration
- **requirement_strength:** not specified
- **issue:** AWS owns SigV4 token generation, but unvalidated successful signer output could be malformed, oversized, wrong-region, or wrongly scoped.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** AWS's maintained signer is the provider authority; alternative non-Java clients also delegate signing, while this adapter adds fail-closed output validation.
- **selected_behavior:** The adapter delegates SigV4 to AWS, then validates the bounded canonical token contract and returns redacted failure categories.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** The validated AWS token is presented through Kafka SASL/OAUTHBEARER; credential material otherwise does not enter Kafka wire data.
- **executable_evidence:** `TestProviderGeneratesOwnedExpiringMSKIAMToken`, `TestSignedTokenValidationRejectsEachMalformedField`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzTokenResultValidation`
- **interoperability_evidence:** `adapters/mskiam/msk_compatibility_test.go`
- **differential_evidence:** `adapters/mskiam/specification/maintained-peer-differential.md`
- **public_apis:** `Provider`, `Token`
- **documentation:** `adapters/mskiam/docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/aws/aws-msk-iam-sasl-signer-go/53637de1b411b2a2c8b2ccb8f103fc1d6b761c07/README.md

## KAFKA-MSKIAM-DEC-002: Credentials refresh per bounded authentication cohort

- **status:** resolved
- **owner:** Kafka MSK IAM adapter maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** AWS SDK for Go v2 credential-provider contract v1.43.0
- **version:** AWS SDK for Go v2 v1.43.0
- **source_authority:** aws-sdk-go-v2-1430
- **section:** CredentialsProvider retrieval and expiry
- **requirement_strength:** not specified
- **issue:** Rotating credentials, concurrent token generation, and caller cancellation can overlap and produce stale tokens or shared cancellation.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained AWS SDK credential providers agree on retrieval and expiry fields but leave outer token refresh coordination to the adapter.
- **selected_behavior:** Authentication retrieves current SDK credentials, shares one bounded near-expiry refresh cohort, caps token expiry, and isolates caller cancellation.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** The validated AWS token is presented through Kafka SASL/OAUTHBEARER; credential material otherwise does not enter Kafka wire data.
- **executable_evidence:** `TestProviderRefreshesExpiringCredentialsAndCapsTokenExpiry`, `TestConcurrentNearExpiryCredentialsRefreshOnce`
- **fixture_evidence:** None.
- **fuzz_evidence:** `FuzzConfigValidate`
- **interoperability_evidence:** `adapters/mskiam/msk_compatibility_test.go`
- **differential_evidence:** `adapters/mskiam/specification/maintained-peer-differential.md`
- **public_apis:** `Config`, `Provider`
- **documentation:** `adapters/mskiam/docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://raw.githubusercontent.com/aws/aws-sdk-go-v2/4fef3455fe2dcb5ea3de4e9fbacf889b84c8a255/go.mod

## KAFKA-MSKIAM-DEC-003: Managed-service support requires direct evidence

- **status:** resolved
- **owner:** Kafka MSK IAM adapter maintainers
- **classification:** interoperability policy
- **decision_scope:** defensive
- **specification:** AWS MSK IAM SASL/OAUTHBEARER signer contract v1.0.4
- **version:** AWS MSK IAM signer v1.0.4
- **source_authority:** aws-msk-guide
- **section:** Configure clients for IAM access control
- **requirement_strength:** not specified
- **issue:** Local signer success cannot prove authentication, authorization, transactions, replay, recovery, or lifecycle behavior on an Amazon MSK mode and version.
- **interpretations:** `Expose the upstream default directly`, `Select and document one bounded package policy`
- **peer_behavior:** Maintained Kafka clients can authenticate through the AWS signer, but service compatibility still requires the same direct managed-service outcome matrix.
- **selected_behavior:** Provisioned and Serverless MSK support remains unverified until the fail-closed live operator gate retains direct service evidence.
- **rationale:** The package must keep the observable policy stable, bounded, and attributable to the pinned authority.
- **security_consequences:** Credentials, payloads, endpoints, and unbounded upstream diagnostics remain outside the public result.
- **resource_consequences:** The selected behavior preserves the package's documented time, memory, concurrency, and input bounds.
- **compatibility_consequences:** Changing this selection requires compatibility review and a new retained decision digest.
- **wire_consequences:** The validated AWS token is presented through Kafka SASL/OAUTHBEARER; credential material otherwise does not enter Kafka wire data.
- **executable_evidence:** `TestNewUsesAWSDefaultCredentialChain`
- **fixture_evidence:** None.
- **fuzz_evidence:** None.
- **interoperability_evidence:** `adapters/mskiam/msk_compatibility_test.go`
- **differential_evidence:** `adapters/mskiam/specification/maintained-peer-differential.md`
- **public_apis:** `Provider`, `New`
- **documentation:** `adapters/mskiam/docs/specification-decisions.md`
- **upstream_status:** The pinned authority defines the protocol or provider boundary; package policy owns the narrower observable contract.
- **reconsider_when:** A monitored authority changes, a maintained peer exposes contrary evidence, or the public contract is deliberately revised.
- **authoritative_url:** https://docs.aws.amazon.com/msk/latest/developerguide/configure-clients-for-iam-access-control.html
