package com.rtbhouse.kafka.workers.impl.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public class TimeUtils {

    public static boolean isOlderThan(Instant instant, Duration maxAge) {
        return isOlderThan(instant, maxAge, Clock.systemUTC());
    }

    private static boolean isOlderThan(Instant instant, Duration maxAge, Clock clock) {
        return isOlderThan(instant, maxAge, Instant.now(clock));
    }

    private static boolean isOlderThan(Instant instant, Duration maxAge, Instant now) {
        return instant.plus(maxAge).isBefore(now);
    }

    public static Duration age(Instant instant) {
        return age(instant, Clock.systemUTC());
    }

    private static Duration age(Instant instant, Clock clock) {
        return Duration.between(instant, Instant.now(clock));
    }
}
