package io.elasticjob.lite.exception;

public class DagJobSysException extends RuntimeException {

    private static final long serialVersionUID = 3244908974343209468L;

    public DagJobSysException(final String errorMessage, final Object... args) {
        super(String.format(errorMessage, args));
    }

    public DagJobSysException(final Throwable cause) {
        super(cause);
    }
}
