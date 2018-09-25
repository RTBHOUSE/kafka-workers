package com.rtbhouse.kafka.workers.api;

public class WorkersException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public WorkersException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkersException(String message) {
        super(message);
    }

    public WorkersException(Throwable cause) {
        super(cause);
    }

}
