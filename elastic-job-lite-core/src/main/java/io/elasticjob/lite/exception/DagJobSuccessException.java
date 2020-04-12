package io.elasticjob.lite.exception;

public class DagJobSuccessException extends RuntimeException {

    private static final long serialVersionUID = 3244908974343209468L;

    public DagJobSuccessException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }

    public DagJobSuccessException(final Throwable cause) {
        super(cause);
    }
}
