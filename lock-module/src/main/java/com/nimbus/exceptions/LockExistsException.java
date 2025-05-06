package com.nimbus.exceptions;

/**
 * Defines an exception which indicates the lock already exists. Does not differentiate owner.
 */
public class LockExistsException extends RuntimeException {
    public LockExistsException(String message) {
        super(message);
    }
}
