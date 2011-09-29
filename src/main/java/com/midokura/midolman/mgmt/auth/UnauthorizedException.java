package com.midokura.midolman.mgmt.auth;

public class UnauthorizedException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Default constructor
     */
    public UnauthorizedException() {
        super();
    }

    public UnauthorizedException(String message) {
        super(message);
    }

    public UnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }
}
