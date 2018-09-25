package com.rtbhouse.kafka.workers.impl.errors;

import com.rtbhouse.kafka.workers.api.WorkersException;

public class BadOffsetException extends WorkersException {

    private static final long serialVersionUID = 1L;

    public BadOffsetException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadOffsetException(String message) {
        super(message);
    }

    public BadOffsetException(Throwable cause) {
        super(cause);
    }
}
