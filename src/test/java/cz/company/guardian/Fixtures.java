package cz.company.guardian;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import io.kroxylicious.kafka.common.compress.Compression;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.ProduceRequestData.PartitionProduceData;
import io.kroxylicious.kafka.common.message.ProduceRequestData.TopicProduceData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.record.internal.MemoryRecords;
import io.kroxylicious.kafka.common.record.internal.SimpleRecord;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import cz.company.guardian.logging.LogRateLimiter;
import cz.company.guardian.logging.RejectLogger;
import cz.company.guardian.metrics.GuardianMetrics;
import cz.company.guardian.validation.RequestValidator;
import cz.company.guardian.validation.SchemaIdSet;
import cz.company.guardian.validation.Whitelist;

/** Builders for requests, records and the shared whitelist used by the unit tests. */
public final class Fixtures {

    public static final String PAYLOAD = "secret-payload-that-must-never-be-logged";

    private Fixtures() {
    }

    /** TEST: 1,2,3 and TEST2: 4,5,6 (the example from the specification), plus MAXID: Integer.MAX_VALUE and ZERO: 0. */
    public static Whitelist whitelist() {
        return Whitelist.of(Map.of(
                "TEST", SchemaIdSet.of(1, 2, 3),
                "TEST2", SchemaIdSet.of(4, 5, 6),
                "MAXID", SchemaIdSet.of(Integer.MAX_VALUE),
                "ZERO", SchemaIdSet.of(0)));
    }

    public static GuardianRuntime runtime(MeterRegistry registry) {
        Whitelist whitelist = whitelist();
        return new GuardianRuntime(
                new RequestValidator(whitelist),
                new GuardianMetrics(registry, whitelist, false, "test"),
                new RejectLogger(new LogRateLimiter(1000)));
    }

    public static MeterRegistry registry() {
        return new SimpleMeterRegistry();
    }

    /** Confluent frame: magic byte 0, big-endian schema id, then an opaque payload. */
    public static byte[] frame(int schemaId) {
        return frame(schemaId, PAYLOAD);
    }

    public static byte[] frame(int schemaId, String payload) {
        byte[] body = payload.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(5 + body.length);
        buffer.put((byte) 0).putInt(schemaId).put(body);
        return buffer.array();
    }

    public static MemoryRecords records(Compression compression, byte[]... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(0L, (byte[]) null, values[i]);
        }
        return MemoryRecords.withRecords(compression, records);
    }

    public static MemoryRecords records(byte[]... values) {
        return records(Compression.NONE, values);
    }

    public static PartitionProduceData partition(int index, MemoryRecords records) {
        return new PartitionProduceData().setIndex(index).setRecords(records);
    }

    public static TopicProduceData topic(String name, PartitionProduceData... partitions) {
        return new TopicProduceData().setName(name).setPartitionData(List.of(partitions));
    }

    public static ProduceRequestData request(short acks, TopicProduceData... topics) {
        ProduceRequestData.TopicProduceDataCollection collection = new ProduceRequestData.TopicProduceDataCollection();
        for (TopicProduceData topic : topics) {
            collection.add(topic);
        }
        return new ProduceRequestData().setAcks(acks).setTimeoutMs(1000).setTopicData(collection);
    }

    public static ProduceRequestData request(TopicProduceData... topics) {
        return request((short) -1, topics);
    }

    /** A request with one topic, one partition and the given values. */
    public static ProduceRequestData simpleRequest(String topic, byte[]... values) {
        return request(topic(topic, partition(0, records(values))));
    }

    public static RequestHeaderData header() {
        return new RequestHeaderData().setCorrelationId(42).setRequestApiVersion((short) 9).setClientId("test-client");
    }
}
