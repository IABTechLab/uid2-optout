package com.uid2.optout;

public class Const extends com.uid2.shared.Const {
    public static class Config extends com.uid2.shared.Const.Config {
        public static final String ServiceVerboseProp = "service_verbose";
        public static final String ServiceInstancesProp = "service_instances";
        public static final String StorageMockProp = "storage_mock";
        public static final String AttestationEncryptionKeyName = "att_token_enc_key";
        public static final String AttestationEncryptionSaltName = "att_token_enc_salt";
        public static final String OptOutInternalApiTokenProp = "optout_internal_api_token";
        public static final String OptOutPartnerEndpointMockProp = "optout_partner_endpoint_mock";
        public static final String OptOutObserveOnlyProp = "optout_observe_only";
        public static final String OptOutS3PathCompatProp = "optout_s3_path_compat";
        public static final String OptOutAddEntryTimeoutMsProp = "optout_add_entry_timeout_ms";
        public static final String OptOutProducerBufferSizeProp = "optout_producer_buffer_size";
        public static final String OptOutSenderReplicaIdProp = "optout_sender_replica_id";
        public static final String OptOutDeleteExpiredProp = "optout_delete_expired";
        public static final String PartnersConfigPathProp = "partners_config_path";
        public static final String PartnersMetadataPathProp = "partners_metadata_path";
        public static final String OptOutSqsQueueUrlProp = "optout_sqs_queue_url";
        public static final String OptOutSqsEnabledProp = "optout_enqueue_sqs_enabled";
    }

    public static class Event {
        public static final String DeltaProduce = "delta.produce";
        public static final String DeltaProduced = "delta.produced";
        public static final String DeltaSentRemote = "delta.sent_remote";
        public static final String PartitionProduce = "partition.produce";
        public static final String PartitionProduced = "partition.produced";
        public static final String EntryAdd = "entry.add";
    }
}
