# Independent modern LZ4 fixtures

Generated from original deterministic plaintext with the reference C LZ4
v1.10.0 CLI (BSD-2-Clause, https://github.com/lz4/lz4/tree/v1.10.0).
These are modern independent-block frames, not standalone legacy frames
or a claim of additional Kafka protocol support. No supplier corpus is copied.

Command: `lz4 -z -q -c -B4 -BI -BX --content-size plaintext.raw`.
Tests reconstruct the original plaintext: empty; ASCII A-Z prefixes of
lengths 3, 5, 7, 15 and 31 repeated 512 times each; that combined payload
repeated five times; and SHA-256 digests of little-endian uint32 values
0 through 255 concatenated. Multiblock exceeds the 64 KiB block size;
stored is incompressible. Each frame was reference-decoded and compared
byte-for-byte to its original plaintext.

| Fixture | Plaintext bytes | Frame SHA-256 |
| --- | ---: | --- |
| empty | 0 | `1829a9507d05badea902f965a15399c2e319389efb3577ddd9a70063d1c68a5b` |
| overlap | 31232 | `9cee49b50b84ebd93fb7368a423855a13f4c64e93bda5bde38f84259c00dbaf1` |
| multiblock | 156160 | `2381ea706c0060e068b762dd595be9445a0305082165d29b07211eeebb54af15` |
| stored | 8192 | `f9efe2fd0713c71ba8b972bce8309c68c2c11cae5f482704890fdeb3ed836aed` |

The Kafka record fixture contains two modern Record encodings with offset
deltas 0/1, key `key`, values `alpha`/`bravo`, and header `hdr: header`.
Attributes and timestamp deltas are zero. Lengths use Kafka zigzag varints;
the batch envelope and CRC are built by the test, independently of LZ4.

| kafka-records | 52 | `d94173c57a13a3e20bd9b7dbc2f73f041462b222da61837905ab22515c5d6609` |
