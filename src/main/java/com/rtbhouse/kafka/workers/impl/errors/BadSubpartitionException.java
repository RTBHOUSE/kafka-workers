package com.rtbhouse.kafka.workers.impl.errors;

import com.rtbhouse.kafka.workers.api.WorkersException;

public class BadSubpartitionException extends WorkersException {

    private static final long serialVersionUID = 1L;

    public BadSubpartitionException(String message, Throwable cause) {
        super(message, cause);
    }

    public BadSubpartitionException(String message) {
        super(message);
    }

    public BadSubpartitionException(Throwable cause) {
        super(cause);
    }
}
