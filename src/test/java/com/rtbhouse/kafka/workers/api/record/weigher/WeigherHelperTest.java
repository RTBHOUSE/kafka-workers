package com.rtbhouse.kafka.workers.api.record.weigher;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class WeigherHelperTest {

    @Test
    @Parameters({
            "java.lang.Object, 16",
            "java.lang.Byte, 24",
            "java.lang.Long, 24",
            "java.lang.String, 48",
            "org.apache.kafka.common.header.internals.RecordHeaders, 40",
            "com.rtbhouse.kafka.workers.api.record.WorkerRecord, 256"
    })
    public void shouldEstimateInstanceSize(String className, int expectedSize) throws ClassNotFoundException {
        // given
        Class<?> clazz = Class.forName(className);

        // when
        int instanceSize = WeigherHelpers.estimateInstanceSize(clazz, "");

        // then
        assertThat(instanceSize).isEqualTo(expectedSize);
    }
}
