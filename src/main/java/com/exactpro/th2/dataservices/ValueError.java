package com.exactpro.th2.dataservices;

public class ValueError extends RuntimeException {
    public ValueError(String message) {
        super(message);
    }

    public ValueError(String message, Throwable cause) {
        super(message, cause);
    }
}
