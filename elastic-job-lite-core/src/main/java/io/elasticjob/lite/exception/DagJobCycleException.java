package io.elasticjob.lite.exception;

public class DagJobCycleException extends RuntimeException {

    private static final long serialVersionUID = 3244908974343209468L;

    public DagJobCycleException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }

    public DagJobCycleException(final Throwable cause) {
        super(cause);
    }
}
