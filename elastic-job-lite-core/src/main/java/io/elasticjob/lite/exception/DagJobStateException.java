package io.elasticjob.lite.exception;

public class DagJobStateException extends RuntimeException {

    private static final long serialVersionUID = 3244908974343209468L;

    public DagJobStateException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }

    public DagJobStateException(final Throwable cause) {
        super(cause);
    }
}
