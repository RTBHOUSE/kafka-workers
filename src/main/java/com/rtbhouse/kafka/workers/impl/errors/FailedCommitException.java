package com.rtbhouse.kafka.workers.impl.errors;

import com.rtbhouse.kafka.workers.api.WorkersException;

public class FailedCommitException extends WorkersException {

    private static final long serialVersionUID = 1L;

    public FailedCommitException(String message, Throwable cause) {
        super(message, cause);
    }

    public FailedCommitException(String message) {
        super(message);
    }

    public FailedCommitException(Throwable cause) {
        super(cause);
    }

}
