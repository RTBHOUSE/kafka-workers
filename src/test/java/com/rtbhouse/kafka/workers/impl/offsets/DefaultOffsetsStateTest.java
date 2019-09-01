package com.rtbhouse.kafka.workers.impl.offsets;

public class DefaultOffsetsStateTest extends OffsetsStateTest {

    @Override
    OffsetsState createOffsetStateSubject() {
        return new DefaultOffsetsState(config, mockMetrics);
    }
}
