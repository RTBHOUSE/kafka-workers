package com.rtbhouse.kafka.workers.impl.errors;

public class RecordProcessingActionException extends RuntimeException {

    public RecordProcessingActionException(Throwable cause) {
        super(cause);
    }

    public RecordProcessingActionException(String message, Throwable cause) {
        super(message, cause);
    }
}
