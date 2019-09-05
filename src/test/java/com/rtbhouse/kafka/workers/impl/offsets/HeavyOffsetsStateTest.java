package com.rtbhouse.kafka.workers.impl.offsets;

public class HeavyOffsetsStateTest extends OffsetsStateTest {

    @Override
    OffsetsState createOffsetsStateSubject() {
        return new HeavyOffsetsState();
    }
}
