# Maintained-peer differential evidence

OpenTelemetry Go v1.44.0 and semantic conventions v1.44.0 are pinned as
separate authorities. Existing adapter tests compare the public observation
contract with the SDK's actual span, metric, provider, and W3C propagator
behavior.

The assessed difference is deliberate: client-hook instrumentations can start
standard messaging spans at lifecycle boundaries unavailable to this adapter,
while this adapter refuses to infer those spans from completion observations.
Provider ownership and identity cardinality likewise remain explicit package
policy rather than being selected by common global-instrumentation defaults.
