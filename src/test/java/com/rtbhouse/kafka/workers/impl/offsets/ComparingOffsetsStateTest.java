package com.rtbhouse.kafka.workers.impl.offsets;

public class ComparingOffsetsStateTest extends OffsetsStateTest {

    @Override
    OffsetsState createOffsetsStateSubject() {
        return new ComparingOffsetsState(
                new DefaultOffsetsState(config, mockMetrics),
                new HeavyOffsetsState()
        );
    }
}
