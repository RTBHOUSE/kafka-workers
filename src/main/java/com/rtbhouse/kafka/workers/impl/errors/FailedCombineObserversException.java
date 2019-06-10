package com.rtbhouse.kafka.workers.impl.errors;

import com.rtbhouse.kafka.workers.api.WorkersException;

public class FailedCombineObserversException extends WorkersException {

    private static final long serialVersionUID = 1L;

    public FailedCombineObserversException(String message, Throwable cause) {
        super(message, cause);
    }

    public FailedCombineObserversException(String message) {
        super(message);
    }

    public FailedCombineObserversException(Throwable cause) {
        super(cause);
    }

}
