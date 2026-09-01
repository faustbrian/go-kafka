# Maintained-peer differential evidence

The maintained AWS MSK IAM signer v1.0.4 and AWS SDK for Go v2 v1.43.0 are
separate pinned authorities. The adapter delegates signing and credential
retrieval to those providers, then differentially checks their output against
the package's bounded token, expiry, refresh, concurrency, and redaction
contract.

The assessed difference is deliberate: alternative non-Java Kafka clients may
accept signer output directly, while this adapter validates canonical region,
host, action, fields, expiry, and size. Direct Provisioned and Serverless MSK
evidence remains a provider-agreement lane and is never inferred from local
signer agreement.
