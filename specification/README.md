# Specification conformance matrix

The [decision register](../docs/specification-decisions.md) binds every observable decision to pinned authorities and current evidence.

| Decision | Authority | Executable evidence | Differential evidence |
| --- | --- | --- | --- |
| KAFKA-DEC-001 | `kafka-431-protocol` | `TestClientRolesApplyProtocolAndBoundedEOFRecoveryPolicy`, `TestClientRolesRejectInvalidProtocolPolicy` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-002 | `kafka-431-protocol` | `TestNewProducerAppliesBoundedIdempotentDeliveryPolicy`, `TestProducerPublishRecordReturnsDeliveryMetadataAndOwnsInput` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-003 | `kafka-kip-429` | `TestConsumerRunOnceCommitsOnlyContiguousPartitionSuccess`, `TestConsumerFencesSettlementAfterAssignmentEpochChanges` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-004 | `kafka-431-protocol` | `TestFailureHandlerPublishFailureDoesNotResolveSource`, `TestFailureHandlerPublishesOwnedDeadLetterMetadata` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-005 | `kafka-kip-447` | `TestRunTransactionRedactsUnknownCommitOutcome`, `TestTransactionProcessorCommitsCompletePollAtomically` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-006 | `kafka-431-protocol` | `TestInspectorPlansReplayTimestampWindowAsOwnedExactRanges`, `TestReplayFailsClosedOnUnexpectedRecordsAndOffsetGaps` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-007 | `kafka-kip-848` | `TestInspectorPreservesRequestAndPartitionFailures`, `TestInspectorReadinessUsesHysteresisWithoutAffectingLiveness` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-008 | `kafka-431-protocol` | `TestClientSecurityDefaultsToVerifiedTLSAndRequiresExplicitPlaintext`, `TestTrustAnchorProviderIsBoundedOwnedRotatingAndPanicSafe` | `specification/maintained-peer-differential.md` |
| KAFKA-DEC-009 | `kafka-431-protocol` | `TestObservationValidationRejectsMetadataOutsideThePublicContract`, `TestObserverFailuresAreContainedAndReportedInOrder` | `specification/maintained-peer-differential.md` |
