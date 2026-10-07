# Cross-version Kafka codec fixtures

The Snappy and Zstd frames were produced with klauspost/compress v1.19.1
(commit 2602f4afea09fe72f2b58d4ed04d43a6047a0131), before adoption of v1.20.1.
The literal plaintext is `Kafka-compatible record payload` followed by a
newline, repeated 4096 times. These are cross-version, not independent-
implementation, interoperability fixtures.

Generate using `s2.EncodeSnappy(nil, plaintext)` and
`zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1)).EncodeAll(plaintext, nil)`
with that pinned module. The tests exercise the real bounded owned decoder.

SHA-256:

- `baseline-snappy.bin`: `9dcc2476bbaf642a23db52fa1b777c13d08ec2d7960deec3255759e3c0b5ce55`
- `baseline-zstd.bin`: `f18f412bec40654d929bd7fbfdcf0b2960c666c16d5ef141f5437fdd16890359`
