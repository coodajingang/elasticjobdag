package io.elasticjob.lite.exception;

public class DagJobPauseException extends RuntimeException {

    private static final long serialVersionUID = 3244908974343209468L;

    public DagJobPauseException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }

    public DagJobPauseException(final Throwable cause) {
        super(cause);
    }
}
