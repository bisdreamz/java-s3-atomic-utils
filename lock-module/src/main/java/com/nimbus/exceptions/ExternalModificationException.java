package com.nimbus.exceptions;

/**
 * Defines an exception thrown when external, unexpected modification happen.
 * E.g. we are holding a successful lock and our lock file is manually deleted
 * or overwritten
 */
public class ExternalModificationException extends RuntimeException {
    public ExternalModificationException(String message) {
        super(message);
    }
    public ExternalModificationException(String message, Throwable cause) {
        super(message, cause);
    }
}
