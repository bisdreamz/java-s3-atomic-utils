package com.nimbus.exceptions;

/**
 * Defines an exception which indicates the lock exists, and is owned by a remote server
 */
public class RemoteLockExistsException extends LockExistsException {
    public RemoteLockExistsException(String message) {
        super(message);
    }
}
