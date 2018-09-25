package com.rtbhouse.kafka.workers.impl.errors;

import com.rtbhouse.kafka.workers.api.WorkersException;

public class ProcessingTimeoutException extends WorkersException {

    private static final long serialVersionUID = 1L;

    public ProcessingTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessingTimeoutException(String message) {
        super(message);
    }

    public ProcessingTimeoutException(Throwable cause) {
        super(cause);
    }
}
