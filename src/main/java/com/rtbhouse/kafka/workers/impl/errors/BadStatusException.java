package com.rtbhouse.kafka.workers.impl.errors;

import com.rtbhouse.kafka.workers.api.WorkersException;

public class BadStatusException extends WorkersException {

    private static final long serialVersionUID = 1L;

    public BadStatusException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadStatusException(String message) {
        super(message);
    }

    public BadStatusException(Throwable cause) {
        super(cause);
    }
}
