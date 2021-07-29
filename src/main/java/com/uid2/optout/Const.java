// Copyright (c) 2021 The Trade Desk, Inc
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
//    this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

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
