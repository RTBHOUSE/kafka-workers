package com.rtbhouse.kafka.workers.api.record.weigher;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class BaseRecordWeigherTest {

    private static final String EMPTY_TOPIC = "";
    private static final int SOME_PARTITION = 0;
    private static final long SOME_OFFSET = 0L;
    private static final long SOME_TIMESTAMP = 0L;
    private static final int SOME_SUBPARTITION = 0;
    private static final char SOME_CHAR = 'A';


    @Test
    @Parameters({
            "0, 0, 280",
            "1, 0, 288",
            "0, 1, 288",
            "7, 0, 288",
            "0, 7, 288",
            "3, 4, 296",
            "4, 3, 296",
            "7, 7, 296",
            "111, 222, 616",
            "222, 111, 616"
    })
    public void shouldWeighByteArrays(int keyLength, int valueLength, long expectedWeight) {
        // given
        WorkerRecord<byte[], byte[]> workerRecord = workerRecordWithBytes(keyLength, valueLength);
        BaseRecordWeigher<byte[], byte[]> recordWeigher = new BaseRecordWeigher<>(
                ByteArrayWeigher.INSTANCE, ByteArrayWeigher.INSTANCE);

        // expect
        assertThat(recordWeigher.weigh(workerRecord)).isEqualTo(expectedWeight);
    }

    @Test
    @Parameters({
            "0, 0, 344",
            "1, 0, 352",
            "0, 1, 352",
            "7, 0, 352",
            "0, 7, 352",
            "3, 4, 360",
            "4, 3, 360",
            "7, 7, 360",
            "111, 222, 680",
            "222, 111, 680"
    })
    public void shouldWeighStrings(int keyLength, int valueLength, long expectedWeight) {
        // given
        WorkerRecord<String, String> workerRecord = workerRecordWithStrings(keyLength, valueLength);
        BaseRecordWeigher<String, String> recordWeigher = new BaseRecordWeigher<>(
                StringWeigher.INSTANCE, StringWeigher.INSTANCE);

        // expect
        assertThat(recordWeigher.weigh(workerRecord)).isEqualTo(expectedWeight);
    }


    @Test
    @Parameters({
            "392, 1:1",
            "392, 123:1234",
            "392, 1234:123",
            "400, 1:123456789",
            "400, 123456789:1",
            "408, 123456789:123456789",
            "504, 1:1, 1:1",
            "616, 1:1, 1:1, 12345678:12345678",
            "632, 1:1, 1:1, 123456789:123456789"
    })
    public void shouldWeighHeaders(long expectedWeight, String[] headers) {
        // given
        WorkerRecord<byte[], byte[]> workerRecord = emptyWorkerRecordWithHeaders(headers);
        BaseRecordWeigher<byte[], byte[]> recordWeigher = new BaseRecordWeigher<>(
                ByteArrayWeigher.INSTANCE, ByteArrayWeigher.INSTANCE);

        // expect
        assertThat(recordWeigher.weigh(workerRecord)).isEqualTo(expectedWeight);
    }

    private WorkerRecord<byte[], byte[]> workerRecordWithBytes(int keyLength, int valueLength) {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(EMPTY_TOPIC, SOME_PARTITION, SOME_OFFSET,
                new byte[keyLength], new byte[valueLength]);
        return new WorkerRecord<>(consumerRecord, SOME_SUBPARTITION);
    }

    private WorkerRecord<String, String> workerRecordWithStrings(int keyLength, int valueLength) {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(EMPTY_TOPIC, SOME_PARTITION, SOME_OFFSET,
                StringUtils.repeat(SOME_CHAR, keyLength), StringUtils.repeat(SOME_CHAR, valueLength));
        return new WorkerRecord<>(consumerRecord, SOME_SUBPARTITION);
    }

    private WorkerRecord<byte[], byte[]> emptyWorkerRecordWithHeaders(String[] headers) {
        RecordHeaders recordHeaders = new RecordHeaders();
        for (String headerStr: headers) {
            String[] split = headerStr.split(":");
            recordHeaders.add(new RecordHeader(split[0], split[1].getBytes(ISO_8859_1)));
        }
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(EMPTY_TOPIC, SOME_PARTITION,
                SOME_OFFSET, SOME_TIMESTAMP,
                TimestampType.NO_TIMESTAMP_TYPE, (long) ConsumerRecord.NULL_CHECKSUM, 0, 0,
                new byte[0], new byte[0],
                recordHeaders);

        return new WorkerRecord<>(consumerRecord, SOME_SUBPARTITION);
    }
}
