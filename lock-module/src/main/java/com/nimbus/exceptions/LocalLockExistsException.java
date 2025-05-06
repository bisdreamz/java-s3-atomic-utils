package com.nimbus.exceptions;

/**
 * Defines an exception which defines that the lock exists, and we are already the owner
 */
public class LocalLockExistsException extends LockExistsException {
    public LocalLockExistsException(String message) {
        super(message);
    }
}
