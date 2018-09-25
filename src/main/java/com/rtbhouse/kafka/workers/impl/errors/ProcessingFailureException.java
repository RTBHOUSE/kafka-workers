package com.rtbhouse.kafka.workers.impl.errors;

import com.rtbhouse.kafka.workers.api.WorkersException;

public class ProcessingFailureException extends WorkersException {

    private static final long serialVersionUID = 1L;

    public ProcessingFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProcessingFailureException(String message) {
        super(message);
    }

    public ProcessingFailureException(Throwable cause) {
        super(cause);
    }

}
